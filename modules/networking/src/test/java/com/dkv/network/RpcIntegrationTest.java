package com.dkv.network;

import com.dkv.raft.RaftNode;
import com.dkv.raft.proto.RaftProtos;
import com.dkv.rpc.proto.RpcProtos.RpcMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class RpcIntegrationTest {
    private NettyServer server;
    private NettyClient client;
    private RaftNode mockRaftNode;
    private final int PORT = 8081;

    @BeforeEach
    public void setup() throws InterruptedException {
        // Simple mock since we want to test networking mainly
        mockRaftNode = new RaftNode("server-node") {
            @Override
            public synchronized RaftProtos.RequestVoteResponse handleRequestVote(
                    RaftProtos.RequestVoteRequest request) {
                return RaftProtos.RequestVoteResponse.newBuilder()
                        .setTerm(request.getTerm())
                        .setVoteGranted(true)
                        .build();
            }
        };

        server = new NettyServer(PORT, mockRaftNode);
        server.start();

        client = new NettyClient();
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        if (client != null)
            client.stop();
        if (server != null) {
            server.stop();
            server.awaitTermination();
        }
    }

    @Test
    public void testRequestVoteRpc() throws ExecutionException, InterruptedException {
        RaftProtos.RequestVoteRequest rvRequest = RaftProtos.RequestVoteRequest.newBuilder()
                .setTerm(5)
                .setCandidateId("client-node")
                .setLastLogIndex(10)
                .setLastLogTerm(4)
                .build();

        RpcMessage request = RpcMessage.newBuilder()
                .setType(RpcMessage.MessageType.REQUEST_VOTE_REQUEST)
                .setRequestId("req-1")
                .setRequestVoteRequest(rvRequest)
                .build();

        RpcMessage response = client.sendRequest("localhost", PORT, request).get();

        assertNotNull(response);
        assertEquals(RpcMessage.MessageType.REQUEST_VOTE_RESPONSE, response.getType());
        assertEquals("req-1", response.getRequestId());
        assertTrue(response.getRequestVoteResponse().getVoteGranted());
        assertEquals(5, response.getRequestVoteResponse().getTerm());
    }
}
