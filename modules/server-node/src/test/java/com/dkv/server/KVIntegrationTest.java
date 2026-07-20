package com.dkv.server;

import com.dkv.kv.proto.KvProtos;
import com.dkv.raft.RaftNode;
import com.dkv.raft.RaftState;
import com.dkv.storage.LSMStorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class KVIntegrationTest {
    private static final String DATA_DIR = "kv-test-data";
    private LSMStorageEngine storageEngine;
    private KVStateMachine stateMachine;
    private RaftNode raftNode;
    private KVRequestHandler requestHandler;

    @BeforeEach
    public void setup() throws IOException {
        cleanup();
        new File(DATA_DIR).mkdirs();

        storageEngine = new LSMStorageEngine(DATA_DIR);
        stateMachine = new KVStateMachine(storageEngine);
        raftNode = new RaftNode("leader-1", stateMachine);
        requestHandler = new KVRequestHandler(raftNode, storageEngine);

        raftNode.start();
        // Manually promote to leader for testing single-node logic
        makeLeader(raftNode);
    }

    private void makeLeader(RaftNode node) {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
        try {
            java.lang.reflect.Field stateField = RaftNode.class.getDeclaredField("state");
            stateField.setAccessible(true);
            stateField.set(node, RaftState.LEADER);

            java.lang.reflect.Field termField = RaftNode.class.getDeclaredField("currentTerm");
            termField.setAccessible(true);
            termField.set(node, 1L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (storageEngine != null)
            storageEngine.close();
        cleanup();
    }

    private void cleanup() {
        File dir = new File(DATA_DIR);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files)
                    f.delete();
            }
            dir.delete();
        }
    }

    @Test
    public void testPutAndGetWithRaft() throws IOException, InterruptedException {
        // 1. Client PUT
        KvProtos.KVRequest putReq = KvProtos.KVRequest.newBuilder()
                .setType(KvProtos.KVRequest.CommandType.PUT)
                .setKey("user1")
                .setValue("Adithya")
                .build();

        KvProtos.KVResponse putResp = requestHandler.handle(putReq);
        assertTrue(putResp.getSuccess());
        assertEquals("Command accepted for replication", putResp.getMessage());

        try {
            java.lang.reflect.Field commitField = RaftNode.class.getDeclaredField("commitIndex");
            commitField.setAccessible(true);
            commitField.set(raftNode, raftNode.getRaftLog().getLastLogIndex());

            // Invoke applyLog private method?
            java.lang.reflect.Method applyMethod = RaftNode.class.getDeclaredMethod("applyLog");
            applyMethod.setAccessible(true);
            applyMethod.invoke(raftNode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 3. Client GET
        KvProtos.KVRequest getReq = KvProtos.KVRequest.newBuilder()
                .setType(KvProtos.KVRequest.CommandType.GET)
                .setKey("user1")
                .build();

        KvProtos.KVResponse getResp = requestHandler.handle(getReq);
        assertTrue(getResp.getSuccess());
        assertEquals("Adithya", getResp.getValue());
    }
}
