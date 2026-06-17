package com.dkv.raft;

import com.dkv.raft.proto.RaftProtos.AppendEntriesRequest;
import com.dkv.raft.proto.RaftProtos.AppendEntriesResponse;
import com.dkv.raft.proto.RaftProtos.InstallSnapshotRequest;
import com.dkv.raft.proto.RaftProtos.InstallSnapshotResponse;
import com.dkv.raft.proto.RaftProtos.RequestVoteRequest;
import com.dkv.raft.proto.RaftProtos.RequestVoteResponse;

/**
 * Represents one other node in the cluster from the perspective of the current node.
 * Provides methods to send Raft RPCs and track replication progress.
 */
public interface RaftPeer {

    /** Returns the unique identifier of this peer. */
    String getId();

    /** Sends a RequestVote RPC to this peer and returns the response. */
    RequestVoteResponse requestVote(RequestVoteRequest request);

    /** Sends an AppendEntries RPC to this peer and returns the response. */
    AppendEntriesResponse appendEntries(AppendEntriesRequest request);

    /** Sends an InstallSnapshot RPC to this peer and returns the response. */
    InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request);

    /** Gets the next log index to send to this peer (leader-maintained). */
    long getNextIndex();

    /** Sets the next log index to send to this peer. */
    void setNextIndex(long nextIndex);

    /** Gets the highest log index known to be replicated on this peer. */
    long getMatchIndex();

    /** Sets the highest log index known to be replicated on this peer. */
    void setMatchIndex(long matchIndex);

    /**
     * Returns true if the last RPC to this peer succeeded.
     * Updated automatically on every requestVote / appendEntries / installSnapshot call.
     */
    boolean isReachable();
}
