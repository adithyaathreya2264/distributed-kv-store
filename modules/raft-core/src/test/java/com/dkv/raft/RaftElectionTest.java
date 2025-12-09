package com.dkv.raft;

import com.dkv.raft.proto.RaftProtos.RequestVoteRequest;
import com.dkv.raft.proto.RaftProtos.RequestVoteResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RaftElectionTest {
    private RaftNode node;

    @BeforeEach
    public void setup() {
        node = new RaftNode("node1");
        node.start();
    }

    @Test
    public void testStartAsFollower() {
        assertEquals(RaftState.FOLLOWER, node.getState());
        assertEquals(0, node.getCurrentTerm());
    }

    @Test
    public void testRejectVoteIfTermIsOld() {
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(0)
                .setCandidateId("node2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        // Node term is 0, request term is 0.
        // Logic: if request.term < currentTerm (false)
        // request.term > currentTerm (false)
        // log is up to date.
        // votedFor is null.
        // Should grant? Yes, same term is allowed if votedFor is null.
        // Wait, if terms are equal and we haven't voted, we can vote.

        RequestVoteResponse response = node.handleRequestVote(request);
        assertTrue(response.getVoteGranted());
        assertEquals(0, response.getTerm());
    }

    @Test
    public void testVoteForHigherTerm() {
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(2)
                .setCandidateId("node2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = node.handleRequestVote(request);

        assertTrue(response.getVoteGranted());
        assertEquals(2, response.getTerm());
        assertEquals(RaftState.FOLLOWER, node.getState());
        assertEquals(2, node.getCurrentTerm());
    }

    @Test
    public void testRejectDuplicateVoteInSameTerm() {
        RequestVoteRequest request1 = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        node.handleRequestVote(request1);

        RequestVoteRequest request2 = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node3") // Different candidate
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = node.handleRequestVote(request2);

        assertFalse(response.getVoteGranted());
        assertEquals(1, response.getTerm());
    }
}
