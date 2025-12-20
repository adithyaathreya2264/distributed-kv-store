package com.dkv.server;

import com.dkv.kv.proto.KvProtos;
import com.dkv.network.NettyServer;
import com.dkv.raft.RaftNode;
import com.dkv.raft.RaftState;
import com.dkv.storage.LSMStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

public class DKVServer {
    private static final Logger logger = LoggerFactory.getLogger(DKVServer.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: DKVServer <nodeId> <port> <dataDir>");
            System.exit(1);
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);
        String dataDir = args[2];

        logger.info("Starting DKVServer Node={} Port={} DataDir={}", nodeId, port, dataDir);

        // 1. Storage Engine
        new File(dataDir).mkdirs();
        LSMStorageEngine storageEngine = new LSMStorageEngine(dataDir);

        // 2. State Machine
        KVStateMachine stateMachine = new KVStateMachine(storageEngine);

        // 3. Raft Node
        RaftNode raftNode = new RaftNode(nodeId, stateMachine);

        // 4. KV Request Handler
        KVRequestHandler requestHandler = new KVRequestHandler(raftNode, storageEngine);

        // 5. Netty Server with injected handlers
        Function<KvProtos.KVRequest, KvProtos.KVResponse> kvHandler = requestHandler::handle;

        NettyServer server = new NettyServer(port, raftNode, kvHandler);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Shutting down...");
                server.stop();
                storageEngine.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        raftNode.start();

        // Force Leader for single-node demo so we can write immediately
        // This stops the election timer and sets state to LEADER
        raftNode.becomeLeader();

        server.start();

        // Block until termination
        server.awaitTermination();
    }
}
