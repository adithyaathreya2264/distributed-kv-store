package com.dkv.network;

import com.dkv.raft.RaftPeer;
import com.dkv.raft.proto.RaftProtos.AppendEntriesRequest;
import com.dkv.raft.proto.RaftProtos.AppendEntriesResponse;
import com.dkv.raft.proto.RaftProtos.InstallSnapshotRequest;
import com.dkv.raft.proto.RaftProtos.InstallSnapshotResponse;
import com.dkv.raft.proto.RaftProtos.RequestVoteRequest;
import com.dkv.raft.proto.RaftProtos.RequestVoteResponse;
import com.dkv.rpc.proto.RpcProtos.RpcMessage;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Concrete RaftPeer that makes real network calls via Netty.
 * Each RPC wraps the Raft Protobuf message inside an RpcMessage envelope,
 * sends it through a shared NettyClient, and blocks for the response.
 * Tracks reachability based on whether the last RPC succeeded or failed.
 */
public class NettyRaftPeer implements RaftPeer {

    private static final long RPC_TIMEOUT_MS = 200;

    private final String nodeId;
    private final String host;
    private final int port;
    private final NettyClient client;

    private volatile long nextIndex = 1;
    private volatile long matchIndex = 0;
    private volatile boolean reachable = true;

    public NettyRaftPeer(String nodeId, String host, int port, NettyClient client) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.client = client;
    }

    @Override
    public String getId() {
        return nodeId;
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        RpcMessage envelope = RpcMessage.newBuilder()
                .setType(RpcMessage.MessageType.REQUEST_VOTE_REQUEST)
                .setRequestId(UUID.randomUUID().toString())
                .setRequestVoteRequest(request)
                .build();

        try {
            RpcMessage response = client.sendRequest(host, port, envelope)
                    .get(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            reachable = true;
            return response.getRequestVoteResponse();
        } catch (TimeoutException e) {
            reachable = false;
            throw new RuntimeException("RequestVote to " + nodeId + " timed out", e);
        } catch (Exception e) {
            reachable = false;
            throw new RuntimeException("RequestVote to " + nodeId + " failed", e);
        }
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        RpcMessage envelope = RpcMessage.newBuilder()
                .setType(RpcMessage.MessageType.APPEND_ENTRIES_REQUEST)
                .setRequestId(UUID.randomUUID().toString())
                .setAppendEntriesRequest(request)
                .build();

        try {
            RpcMessage response = client.sendRequest(host, port, envelope)
                    .get(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            reachable = true;
            return response.getAppendEntriesResponse();
        } catch (TimeoutException e) {
            reachable = false;
            throw new RuntimeException("AppendEntries to " + nodeId + " timed out", e);
        } catch (Exception e) {
            reachable = false;
            throw new RuntimeException("AppendEntries to " + nodeId + " failed", e);
        }
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        RpcMessage envelope = RpcMessage.newBuilder()
                .setType(RpcMessage.MessageType.INSTALL_SNAPSHOT_REQUEST)
                .setRequestId(UUID.randomUUID().toString())
                .setInstallSnapshotRequest(request)
                .build();

        try {
            // Longer timeout for snapshots which can be large
            RpcMessage response = client.sendRequest(host, port, envelope)
                    .get(2000, TimeUnit.MILLISECONDS);
            reachable = true;
            return response.getInstallSnapshotResponse();
        } catch (TimeoutException e) {
            reachable = false;
            throw new RuntimeException("InstallSnapshot to " + nodeId + " timed out", e);
        } catch (Exception e) {
            reachable = false;
            throw new RuntimeException("InstallSnapshot to " + nodeId + " failed", e);
        }
    }

    @Override
    public long getNextIndex() {
        return nextIndex;
    }

    @Override
    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    @Override
    public long getMatchIndex() {
        return matchIndex;
    }

    @Override
    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    @Override
    public boolean isReachable() {
        return reachable;
    }
}
