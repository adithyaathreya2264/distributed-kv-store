package com.dkv.raft;

import com.dkv.raft.proto.RaftProtos.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private static final int HEARTBEAT_INTERVAL_MS = 50;
    private static final int PROPOSE_TIMEOUT_MS = 2000;
    private static final int SNAPSHOT_THRESHOLD = 1000;

    private volatile RaftState state = RaftState.FOLLOWER;
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private final String nodeId;
    private final RaftLog raftLog;
    private final RaftTimer electionTimer;

    private final StateMachine stateMachine;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    private List<RaftPeer> peers = new ArrayList<>();

    // Data directory for persisting metadata and snapshots
    private final String dataDir;
    private static final String METADATA_FILE = "raft_metadata.properties";
    private static final String SNAPSHOT_FILE = "snapshot.bin";
    private static final String SNAPSHOT_TMP_FILE = "snapshot.bin.tmp";

    // Thread pool for sending RPCs to peers in parallel
    private final ExecutorService rpcExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "raft-rpc");
        t.setDaemon(true);
        return t;
    });

    // Scheduler for periodic heartbeats
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "raft-heartbeat");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> heartbeatTask;

    public RaftNode(String nodeId, StateMachine stateMachine, String dataDir) {
        this.nodeId = nodeId;
        this.stateMachine = stateMachine;
        this.dataDir = dataDir;
        this.raftLog = new RaftLog();
        // Election timeout between 150ms and 300ms
        this.electionTimer = new RaftTimer(this::startElection, 150, 300);
    }

    public RaftNode(String nodeId, StateMachine stateMachine) {
        this(nodeId, stateMachine, null);
    }

    // For testing/compatibility without StateMachine
    public RaftNode(String nodeId) {
        this(nodeId, new StateMachine() {
            @Override
            public void apply(byte[] command) {}
            @Override
            public Map<String, String> getSnapshotData() { return Collections.emptyMap(); }
            @Override
            public void restoreFromSnapshot(Map<String, String> snapshotData) {}
        }, null);
    }

    /**
     * Sets the list of peers this node knows about.
     * Must be called before start().
     */
    public void setPeers(List<RaftPeer> peers) {
        this.peers = new ArrayList<>(peers);
        logger.info("Node {} configured with {} peer(s): {}", nodeId, peers.size(),
                peers.stream().map(RaftPeer::getId).toList());
    }

    public void start() {
        logger.info("Starting RaftNode {}", nodeId);
        // Restore persisted state before starting election timer
        loadMetadata();
        loadSnapshot();
        electionTimer.reset();
    }

    /**
     * Shuts down background executors gracefully.
     */
    public void stop() {
        electionTimer.stop();
        stopHeartbeat();
        rpcExecutor.shutdownNow();
        heartbeatScheduler.shutdownNow();
    }

    // ========== Persistence ==========

    /**
     * Persists currentTerm and votedFor to disk. Must be called synchronously
     * before sending any RPC response when either value changes.
     */
    private void persistMetadata() {
        if (dataDir == null) return;
        File metaFile = new File(dataDir, METADATA_FILE);
        try (FileOutputStream fos = new FileOutputStream(metaFile)) {
            String content = "currentTerm=" + currentTerm + "\n"
                    + "votedFor=" + (votedFor != null ? votedFor : "") + "\n";
            fos.write(content.getBytes(StandardCharsets.UTF_8));
            fos.getFD().sync();
        } catch (IOException e) {
            logger.error("Failed to persist Raft metadata", e);
        }
    }

    /**
     * Restores currentTerm and votedFor from disk on startup.
     */
    private void loadMetadata() {
        if (dataDir == null) return;
        File metaFile = new File(dataDir, METADATA_FILE);
        if (!metaFile.exists()) return;
        try {
            Properties props = new Properties();
            try (FileInputStream fis = new FileInputStream(metaFile)) {
                props.load(fis);
            }
            String termStr = props.getProperty("currentTerm");
            if (termStr != null && !termStr.isEmpty()) {
                currentTerm = Long.parseLong(termStr);
            }
            String vf = props.getProperty("votedFor");
            if (vf != null && !vf.isEmpty()) {
                votedFor = vf;
            }
            logger.info("Node {} restored metadata: term={}, votedFor={}", nodeId, currentTerm, votedFor);
        } catch (IOException e) {
            logger.error("Failed to load Raft metadata", e);
        }
    }

    // ========== Snapshotting ==========

    /**
     * Loads a snapshot from disk on startup, restoring the state machine
     * and log offset.
     */
    private void loadSnapshot() {
        if (dataDir == null) return;
        File snapFile = new File(dataDir, SNAPSHOT_FILE);
        if (!snapFile.exists()) return;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(snapFile))) {
            long snapIndex = dis.readLong();
            long snapTerm = dis.readLong();
            int kvCount = dis.readInt();
            Map<String, String> kvs = new HashMap<>();
            for (int i = 0; i < kvCount; i++) {
                String key = dis.readUTF();
                String value = dis.readUTF();
                kvs.put(key, value);
            }
            stateMachine.restoreFromSnapshot(kvs);
            raftLog.resetAfterSnapshot(snapIndex, snapTerm);
            commitIndex = snapIndex;
            lastApplied = snapIndex;
            logger.info("Node {} loaded snapshot: lastIncludedIndex={}, lastIncludedTerm={}, keys={}",
                    nodeId, snapIndex, snapTerm, kvCount);
        } catch (IOException e) {
            logger.error("Failed to load snapshot", e);
        }
    }

    /**
     * Checks if enough entries have been applied since the last snapshot
     * to warrant creating a new one.
     */
    private void checkSnapshotThreshold() {
        if (dataDir == null) return;
        if (lastApplied - raftLog.getLastIncludedIndex() >= SNAPSHOT_THRESHOLD) {
            takeSnapshot();
        }
    }

    /**
     * Takes a snapshot of the current state machine state, writes it to disk,
     * and truncates the log behind the snapshot point.
     */
    private void takeSnapshot() {
        if (dataDir == null) return;
        long snapIndex = lastApplied;
        LogEntry snapEntry = raftLog.getEntry(snapIndex);
        long snapTerm = (snapEntry != null) ? snapEntry.getTerm() : raftLog.getLastIncludedTerm();

        Map<String, String> snapshotData = stateMachine.getSnapshotData();

        File tmpFile = new File(dataDir, SNAPSHOT_TMP_FILE);
        File snapFile = new File(dataDir, SNAPSHOT_FILE);

        try (FileOutputStream fos = new FileOutputStream(tmpFile);
             DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeLong(snapIndex);
            dos.writeLong(snapTerm);
            dos.writeInt(snapshotData.size());
            for (Map.Entry<String, String> entry : snapshotData.entrySet()) {
                dos.writeUTF(entry.getKey());
                dos.writeUTF(entry.getValue());
            }
            dos.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            logger.error("Failed to write snapshot", e);
            return;
        }

        // Atomic rename
        try {
            Files.move(tmpFile.toPath(), snapFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.error("Failed to rename snapshot file", e);
            return;
        }

        raftLog.truncateBefore(snapIndex, snapTerm);
        logEvent("SNAPSHOT_TAKEN", "index=" + snapIndex + " term=" + snapTerm + " keys=" + snapshotData.size());
    }

    // ========== Propose ==========

    public synchronized boolean propose(byte[] command) {
        if (state != RaftState.LEADER) {
            return false;
        }
        long term = currentTerm;
        long index = raftLog.getLastLogIndex() + 1;
        LogEntry entry = LogEntry.newBuilder()
                .setTerm(term)
                .setIndex(index)
                .setCommand(com.google.protobuf.ByteString.copyFrom(command))
                .setType(LogEntry.EntryType.DATA)
                .build();

        raftLog.append(entry);
        logEvent("PROPOSAL_RECEIVED", "index=" + index + " term=" + term);

        // If no peers (single-node cluster), commit immediately
        if (peers.isEmpty()) {
            commitIndex = index;
            applyLog();
            return true;
        }

        // Trigger immediate replication to all peers
        sendHeartbeats();

        // Wait for quorum commit (blocking)
        long deadline = System.currentTimeMillis() + PROPOSE_TIMEOUT_MS;
        while (commitIndex < index && System.currentTimeMillis() < deadline) {
            try {
                wait(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return commitIndex >= index;
    }

    // ========== Elections ==========

    private void startElection() {
        synchronized (this) {
            if (state == RaftState.LEADER) {
                return;
            }
            state = RaftState.CANDIDATE;
            currentTerm++;
            votedFor = nodeId;
            persistMetadata();
            logEvent("START_ELECTION", "term=" + currentTerm);
            electionTimer.reset();

            // If no peers, we are the only node — promote immediately
            if (peers.isEmpty()) {
                becomeLeader();
                return;
            }
        }

        // Capture values under lock for use outside
        final long electionTerm;
        final long lastLogIndex;
        final long lastLogTerm;
        synchronized (this) {
            electionTerm = currentTerm;
            lastLogIndex = raftLog.getLastLogIndex();
            lastLogTerm = raftLog.getLastLogTerm();
        }

        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(electionTerm)
                .setCandidateId(nodeId)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm)
                .build();

        // We already voted for ourselves
        AtomicInteger votesGranted = new AtomicInteger(1);
        int majority = (peers.size() + 1) / 2 + 1; // +1 for self

        for (RaftPeer peer : peers) {
            rpcExecutor.submit(() -> {
                try {
                    RequestVoteResponse response = peer.requestVote(request);
                    synchronized (this) {
                        if (state != RaftState.CANDIDATE || currentTerm != electionTerm) {
                            return;
                        }
                        if (response.getTerm() > currentTerm) {
                            currentTerm = response.getTerm();
                            state = RaftState.FOLLOWER;
                            votedFor = null;
                            persistMetadata();
                            logEvent("STATE_CHANGE", "from=CANDIDATE to=FOLLOWER reason=higher_term_seen term=" + currentTerm);
                            electionTimer.reset();
                            return;
                        }
                        if (response.getVoteGranted()) {
                            logEvent("VOTE_GRANTED", "from=" + peer.getId() + " term=" + electionTerm);
                            int votes = votesGranted.incrementAndGet();
                            if (votes >= majority && state == RaftState.CANDIDATE) {
                                becomeLeader();
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to get vote from {}: {}", peer.getId(), e.getMessage());
                }
            });
        }
    }

    public synchronized void becomeLeader() {
        if (state == RaftState.LEADER) {
            return;
        }
        state = RaftState.LEADER;
        logEvent("BECAME_LEADER", "term=" + currentTerm + " peers=" + peers.size());
        electionTimer.stop();

        // Initialize nextIndex and matchIndex for all peers (Raft §5.3)
        long lastLogIndex = raftLog.getLastLogIndex();
        for (RaftPeer peer : peers) {
            peer.setNextIndex(lastLogIndex + 1);
            peer.setMatchIndex(0);
        }

        startHeartbeat();
    }

    private void startHeartbeat() {
        stopHeartbeat();
        heartbeatTask = heartbeatScheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                0,
                HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }

    // ========== Replication ==========

    private void sendHeartbeats() {
        if (state != RaftState.LEADER) {
            return;
        }
        for (RaftPeer peer : peers) {
            rpcExecutor.submit(() -> replicateTo(peer));
        }
    }

    private void replicateTo(RaftPeer peer) {
        try {
            long nextIdx;
            long leaderTerm;
            long leaderCommit;
            long lastIncludedIdx;

            synchronized (this) {
                if (state != RaftState.LEADER) return;
                leaderTerm = currentTerm;
                leaderCommit = commitIndex;
                nextIdx = peer.getNextIndex();
                lastIncludedIdx = raftLog.getLastIncludedIndex();
            }

            // If peer needs entries we've already compacted, send snapshot instead
            if (nextIdx <= lastIncludedIdx) {
                sendSnapshotTo(peer, leaderTerm);
                return;
            }

            // Build prevLogIndex / prevLogTerm
            long prevLogIndex = nextIdx - 1;
            long prevLogTerm = 0;
            if (prevLogIndex > 0) {
                if (prevLogIndex == lastIncludedIdx) {
                    prevLogTerm = raftLog.getLastIncludedTerm();
                } else {
                    LogEntry prevEntry = raftLog.getEntry(prevLogIndex);
                    if (prevEntry != null) {
                        prevLogTerm = prevEntry.getTerm();
                    }
                }
            }

            List<LogEntry> entries = raftLog.getEntriesFrom(nextIdx);

            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                    .setTerm(leaderTerm)
                    .setLeaderId(nodeId)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .addAllEntries(entries)
                    .setLeaderCommit(leaderCommit)
                    .build();

            AppendEntriesResponse response = peer.appendEntries(request);

            synchronized (this) {
                if (state != RaftState.LEADER || currentTerm != leaderTerm) return;

                if (response.getTerm() > currentTerm) {
                    currentTerm = response.getTerm();
                    state = RaftState.FOLLOWER;
                    votedFor = null;
                    persistMetadata();
                    stopHeartbeat();
                    electionTimer.reset();
                    return;
                }

                if (response.getSuccess()) {
                    if (!entries.isEmpty()) {
                        long lastSentIndex = entries.get(entries.size() - 1).getIndex();
                        peer.setNextIndex(lastSentIndex + 1);
                        peer.setMatchIndex(lastSentIndex);
                    }
                    advanceCommitIndex();
                } else {
                    // Accelerated backtracking
                    long conflictIndex = response.getConflictIndex();
                    if (conflictIndex > 0) {
                        peer.setNextIndex(conflictIndex);
                    } else {
                        peer.setNextIndex(Math.max(1, peer.getNextIndex() - 1));
                    }
                }
            }

        } catch (Exception e) {
            logger.warn("Failed to replicate to {}: {}", peer.getId(), e.getMessage());
        }
    }

    /**
     * Sends an InstallSnapshot RPC to a lagging peer.
     */
    private void sendSnapshotTo(RaftPeer peer, long leaderTerm) {
        if (dataDir == null) return;
        File snapFile = new File(dataDir, SNAPSHOT_FILE);
        if (!snapFile.exists()) return;

        try {
            byte[] snapshotBytes = Files.readAllBytes(snapFile.toPath());
            long lastIncIdx = raftLog.getLastIncludedIndex();
            long lastIncTerm = raftLog.getLastIncludedTerm();

            InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder()
                    .setTerm(leaderTerm)
                    .setLeaderId(nodeId)
                    .setLastIncludedIndex(lastIncIdx)
                    .setLastIncludedTerm(lastIncTerm)
                    .setData(com.google.protobuf.ByteString.copyFrom(snapshotBytes))
                    .build();

            InstallSnapshotResponse response = peer.installSnapshot(request);

            synchronized (this) {
                if (response.getTerm() > currentTerm) {
                    currentTerm = response.getTerm();
                    state = RaftState.FOLLOWER;
                    votedFor = null;
                    persistMetadata();
                    stopHeartbeat();
                    electionTimer.reset();
                    return;
                }
                // Update peer tracking
                peer.setNextIndex(lastIncIdx + 1);
                peer.setMatchIndex(lastIncIdx);
            }
            logger.info("Sent snapshot to {} (lastIncludedIndex={})", peer.getId(), lastIncIdx);
        } catch (Exception e) {
            logger.warn("Failed to send snapshot to {}: {}", peer.getId(), e.getMessage());
        }
    }

    private void advanceCommitIndex() {
        long lastLogIndex = raftLog.getLastLogIndex();
        for (long n = lastLogIndex; n > commitIndex; n--) {
            LogEntry entry = raftLog.getEntry(n);
            if (entry == null || entry.getTerm() != currentTerm) {
                continue;
            }
            int replicatedCount = 1; // self
            for (RaftPeer peer : peers) {
                if (peer.getMatchIndex() >= n) {
                    replicatedCount++;
                }
            }
            int majority = (peers.size() + 1) / 2 + 1;
            if (replicatedCount >= majority) {
                commitIndex = n;
                applyLog();
                notifyAll();
                break;
            }
        }
    }

    // ========== RPC Handlers ==========

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        RequestVoteResponse.Builder response = RequestVoteResponse.newBuilder();

        if (request.getTerm() < currentTerm) {
            return response.setTerm(currentTerm).setVoteGranted(false).build();
        }

        if (request.getTerm() > currentTerm) {
            currentTerm = request.getTerm();
            state = RaftState.FOLLOWER;
            votedFor = null;
            stopHeartbeat();
            electionTimer.reset();
        }

        boolean logIsUpToDate = checkLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm());

        if ((votedFor == null || votedFor.equals(request.getCandidateId())) && logIsUpToDate) {
            votedFor = request.getCandidateId();
            state = RaftState.FOLLOWER;
            persistMetadata(); // Write before responding
            electionTimer.reset();
            logEvent("VOTE_GRANTED", "to=" + request.getCandidateId() + " term=" + currentTerm);
            return response.setTerm(currentTerm).setVoteGranted(true).build();
        }

        persistMetadata(); // term may have changed
        return response.setTerm(currentTerm).setVoteGranted(false).build();
    }

    private boolean checkLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = raftLog.getLastLogTerm();
        long myLastIndex = raftLog.getLastLogIndex();

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        AppendEntriesResponse.Builder response = AppendEntriesResponse.newBuilder();

        // 1. Reply false if term < currentTerm
        if (request.getTerm() < currentTerm) {
            return response.setTerm(currentTerm).setSuccess(false).build();
        }

        // Keep alive: valid leader found
        if (request.getTerm() > currentTerm) {
            currentTerm = request.getTerm();
            votedFor = null;
            persistMetadata();
        }
        state = RaftState.FOLLOWER;
        stopHeartbeat();
        electionTimer.reset();

        // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term
        // matches prevLogTerm
        if (request.getPrevLogIndex() > 0) {
            // If prevLogIndex matches the snapshot boundary, use snapshot term
            if (request.getPrevLogIndex() == raftLog.getLastIncludedIndex()) {
                if (raftLog.getLastIncludedTerm() != request.getPrevLogTerm()) {
                    return response.setTerm(currentTerm).setSuccess(false)
                            .setConflictIndex(raftLog.getLastIncludedIndex())
                            .setConflictTerm(0)
                            .build();
                }
            } else if (request.getPrevLogIndex() < raftLog.getLastIncludedIndex()) {
                // prevLogIndex is behind our snapshot — we already have it
                // This is unusual; just report success and let entries be re-applied
            } else {
                LogEntry prevEntry = raftLog.getEntry(request.getPrevLogIndex());
                if (prevEntry == null) {
                    // We don't have this entry at all — conflict at our last index + 1
                    return response.setTerm(currentTerm).setSuccess(false)
                            .setConflictTerm(0)
                            .setConflictIndex(raftLog.getLastLogIndex() + 1)
                            .build();
                }
                if (prevEntry.getTerm() != request.getPrevLogTerm()) {
                    // Conflict: find first index of the conflicting term
                    long conflictTerm = prevEntry.getTerm();
                    long conflictIndex = request.getPrevLogIndex();
                    // Walk backwards to find first index of this term
                    while (conflictIndex > raftLog.getLastIncludedIndex() + 1) {
                        LogEntry e = raftLog.getEntry(conflictIndex - 1);
                        if (e == null || e.getTerm() != conflictTerm) break;
                        conflictIndex--;
                    }
                    return response.setTerm(currentTerm).setSuccess(false)
                            .setConflictTerm(conflictTerm)
                            .setConflictIndex(conflictIndex)
                            .build();
                }
            }
        }

        // 3 & 4. Handle conflicts and append new entries
        for (LogEntry entry : request.getEntriesList()) {
            LogEntry existing = raftLog.getEntry(entry.getIndex());
            if (existing != null && existing.getTerm() != entry.getTerm()) {
                raftLog.truncateFrom(entry.getIndex());
            }
            if (raftLog.getEntry(entry.getIndex()) == null) {
                raftLog.append(entry);
            }
        }

        // 5. Advance commitIndex
        if (request.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(request.getLeaderCommit(), raftLog.getLastLogIndex());
            applyLog();
        }

        return response.setTerm(currentTerm).setSuccess(true).setMatchIndex(raftLog.getLastLogIndex()).build();
    }

    /**
     * Handles an InstallSnapshot RPC from the leader.
     */
    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        InstallSnapshotResponse.Builder response = InstallSnapshotResponse.newBuilder();

        if (request.getTerm() < currentTerm) {
            return response.setTerm(currentTerm).build();
        }

        if (request.getTerm() > currentTerm) {
            currentTerm = request.getTerm();
            votedFor = null;
            persistMetadata();
        }
        state = RaftState.FOLLOWER;
        stopHeartbeat();
        electionTimer.reset();

        // Write snapshot to disk
        if (dataDir != null) {
            File tmpFile = new File(dataDir, SNAPSHOT_TMP_FILE);
            File snapFile = new File(dataDir, SNAPSHOT_FILE);
            try {
                Files.write(tmpFile.toPath(), request.getData().toByteArray());
                Files.move(tmpFile.toPath(), snapFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                logger.error("Failed to write received snapshot", e);
                return response.setTerm(currentTerm).build();
            }
        }

        // Restore state machine from snapshot data
        try (DataInputStream dis = new DataInputStream(
                new ByteArrayInputStream(request.getData().toByteArray()))) {
            long snapIndex = dis.readLong();
            long snapTerm = dis.readLong();
            int kvCount = dis.readInt();
            Map<String, String> kvs = new HashMap<>();
            for (int i = 0; i < kvCount; i++) {
                String key = dis.readUTF();
                String value = dis.readUTF();
                kvs.put(key, value);
            }
            stateMachine.restoreFromSnapshot(kvs);
            raftLog.resetAfterSnapshot(snapIndex, snapTerm);
            commitIndex = snapIndex;
            lastApplied = snapIndex;
            logEvent("SNAPSHOT_INSTALLED", "lastIncludedIndex=" + snapIndex + " keys=" + kvCount);
        } catch (IOException e) {
            logger.error("Failed to parse snapshot data", e);
        }

        return response.setTerm(currentTerm).build();
    }

    private void applyLog() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry entry = raftLog.getEntry(lastApplied);
            if (entry != null && entry.getType() == LogEntry.EntryType.DATA) {
                logEvent("ENTRY_COMMITTED", "index=" + lastApplied);
                stateMachine.apply(entry.getCommand().toByteArray());
            }
        }
        checkSnapshotThreshold();
    }

    // ========== Structured Logging ==========

    /**
     * Emits a single JSON log line with standard Raft event fields.
     * Format: {"ts":"...","node":"...","term":N,"state":"...","event":"...","detail":"..."}
     */
    private void logEvent(String event, String detail) {
        String ts = java.time.Instant.now().toString();
        String json = String.format(
                "{\"ts\":\"%s\",\"node\":\"%s\",\"term\":%d,\"state\":\"%s\",\"event\":\"%s\",\"detail\":\"%s\"}",
                ts, nodeId, currentTerm, state.name(), event,
                detail == null ? "" : detail.replace("\"", "'"));
        logger.info(json);
    }

    // ========== Getters ==========

    public RaftState getState() {
        return state;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public String getNodeId() {
        return nodeId;
    }

    /** Returns an unmodifiable view of the peer list for metrics reporting. */
    public List<RaftPeer> getPeers() {
        return Collections.unmodifiableList(peers);
    }
}