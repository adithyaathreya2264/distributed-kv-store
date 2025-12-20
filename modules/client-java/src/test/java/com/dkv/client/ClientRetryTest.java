package com.dkv.client;

import com.dkv.kv.proto.KvProtos;
import com.dkv.network.NettyServer;
import com.dkv.raft.RaftNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class ClientRetryTest {
    private NettyServer server1;
    private NettyServer server2;
    private DKVClient client;
    private final int PORT1 = 8091;
    private final int PORT2 = 8092;

    @BeforeEach
    public void setup() throws InterruptedException {
        // Mock RaftNode 1: Follower
        RaftNode node1 = new RaftNode("node1") {
            // Minimal override
        };

        // Mock RaftNode 2: Leader
        RaftNode node2 = new RaftNode("node2") {
            // Minimal override
        };

        // Node 1 Handler: Returns "Not Leader"
        Function<KvProtos.KVRequest, KvProtos.KVResponse> handler1 = req -> KvProtos.KVResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Not Leader") // Matches string in DKVClient
                .build();

        // Node 2 Handler: Returns Success
        Function<KvProtos.KVRequest, KvProtos.KVResponse> handler2 = req -> KvProtos.KVResponse.newBuilder()
                .setSuccess(true)
                .setValue("SuccessValue")
                .build();

        // Inject handlers
        server1 = new NettyServer(PORT1, node1, handler1);
        server1.start();

        server2 = new NettyServer(PORT2, node2, handler2);
        server2.start();

        client = new DKVClient(Arrays.asList("localhost:" + PORT1, "localhost:" + PORT2));
    }

    @AfterEach
    public void tearDown() {
        if (client != null)
            client.close();
        if (server1 != null)
            server1.stop();
        if (server2 != null)
            server2.stop();
    }

    @Test
    public void testRetryOnNotLeader() throws ExecutionException, InterruptedException {
        // Should try Node 1 first (index 0), fail with "Not Leader", retry Node 2,
        // succeed.
        String result = client.get("someKey").get();
        assertEquals("SuccessValue", result);
    }
}
