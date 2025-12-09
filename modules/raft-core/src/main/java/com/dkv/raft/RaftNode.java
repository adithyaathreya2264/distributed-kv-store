package com.dkv.raft;

import com.dkv.raft.proto.RaftProtos.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private RaftState state = RaftState.FOLLOWER;
    private long currentTerm = 0;
    private String votedFor = null;
    private final String nodeId;
    private final RaftLog raftLog;
    private final RaftTimer electionTimer;

    private final StateMachine stateMachine;
    private long commitIndex = 0;
    private long lastApplied = 0;

    public RaftNode(String nodeId, StateMachine stateMachine) {
        this.nodeId = nodeId;
        this.stateMachine = stateMachine;
        this.raftLog = new RaftLog();
        // Election timeout between 150ms and 300ms
        this.electionTimer = new RaftTimer(this::startElection, 150, 300);
    }

    // For testing/compatibility without StateMachine
    public RaftNode(String nodeId) {
        this(nodeId, command -> {
        });
    }

    public void start() {
        logger.info("Starting RaftNode {}", nodeId);
        electionTimer.reset();
    }

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
        logger.info("Leader {} proposed command at index {}, term {}", nodeId, index, term);

        // SIMPLE IMPLEMENTATION: If we are a single node (or for this phase's demo),
        // we assume immediate commit so the KV Store works.
        // In a real cluster, we wait for acks.
        commitIndex = index;
        applyLog();

        return true;
    }

    private void startElection() {
        if (state == RaftState.LEADER) {
            return;
        }
        state = RaftState.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        logger.info("Node {} starting election for term {}", nodeId, currentTerm);
        // TODO: Send RequestVote RPCs
        electionTimer.reset();
    }

    /**
     * Forces this node to become Leader.
     * Starts sending heartbeats (log replication) and stops election timer.
     * This is useful for single-node mode or upon winning an election.
     */
    public synchronized void becomeLeader() {
        if (state == RaftState.LEADER) {
            return;
        }
        state = RaftState.LEADER;
        logger.info("Node {} became LEADER at term {}", nodeId, currentTerm);
        electionTimer.stop();
        // TODO: Start Heartbeat Timer
        // For now, valid for single node.
    }

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        RequestVoteResponse.Builder response = RequestVoteResponse.newBuilder();

        // 1. Reply false if term < currentTerm
        if (request.getTerm() < currentTerm) {
            return response.setTerm(currentTerm).setVoteGranted(false).build();
        }

        // If RPC request contains term > currentTerm: set currentTerm = term, convert
        // to follower
        if (request.getTerm() > currentTerm) {
            currentTerm = request.getTerm();
            state = RaftState.FOLLOWER;
            votedFor = null;
            electionTimer.reset();
        }

        // 2. If votedFor is null or candidateId, and candidate’s log is at least as
        // up-to-date as receiver’s log, grant vote
        boolean logIsUpToDate = checkLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm());

        if ((votedFor == null || votedFor.equals(request.getCandidateId())) && logIsUpToDate) {
            votedFor = request.getCandidateId();
            state = RaftState.FOLLOWER; // Ensure we stay follower if we vote
            electionTimer.reset(); // Granting vote resets timer
            logger.info("Node {} granted vote to {} for term {}", nodeId, request.getCandidateId(), currentTerm);
            return response.setTerm(currentTerm).setVoteGranted(true).build();
        }

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
        currentTerm = request.getTerm();
        state = RaftState.FOLLOWER;
        electionTimer.reset();

        // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
        // matches prevLogTerm
        if (request.getPrevLogIndex() > 0) {
            LogEntry prevEntry = raftLog.getEntry(request.getPrevLogIndex());
            if (prevEntry == null || prevEntry.getTerm() != request.getPrevLogTerm()) {
                return response.setTerm(currentTerm).setSuccess(false).build();
            }
        }

        // 3. If an existing entry conflicts with a new one (same index but different
        // terms), delete the existing entry and all that follow
        // 4. Append any new entries not already in the log
        for (LogEntry entry : request.getEntriesList()) {
            LogEntry existing = raftLog.getEntry(entry.getIndex());
            if (existing != null && existing.getTerm() != entry.getTerm()) {
                raftLog.truncateFrom(entry.getIndex());
            }
            if (raftLog.getEntry(entry.getIndex()) == null) {
                raftLog.append(entry);
            }
        }

        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
        // of last new entry)
        if (request.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(request.getLeaderCommit(), raftLog.getLastLogIndex());
            applyLog();
        }

        return response.setTerm(currentTerm).setSuccess(true).setMatchIndex(raftLog.getLastLogIndex()).build();
    }

    private void applyLog() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry entry = raftLog.getEntry(lastApplied);
            if (entry != null && entry.getType() == LogEntry.EntryType.DATA) {
                logger.info("Node {} applying entry at index {}", nodeId, lastApplied);
                stateMachine.apply(entry.getCommand().toByteArray());
            }
        }
    }

    // Getters for testing
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
}
