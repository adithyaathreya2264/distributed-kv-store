package com.dkv.server;

import com.dkv.kv.proto.KvProtos;
import com.dkv.network.NettyClient;
import com.dkv.network.NettyRaftPeer;
import com.dkv.network.NettyServer;
import com.dkv.raft.RaftNode;
import com.dkv.raft.RaftPeer;
import com.dkv.storage.LSMStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DKVServer {
    private static final Logger logger = LoggerFactory.getLogger(DKVServer.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: DKVServer <nodeId> <port> <dataDir> [peers]");
            System.err.println("  peers format: nodeId:host:port,nodeId:host:port,...");
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
        RaftNode raftNode = new RaftNode(nodeId, stateMachine, dataDir);

        // 4. Parse peers and create NettyRaftPeer instances
        NettyClient nettyClient = new NettyClient();
        List<RaftPeer> peers = new ArrayList<>();

        if (args.length >= 4 && !args[3].isEmpty()) {
            String peersArg = args[3];
            for (String chunk : peersArg.split(",")) {
                String[] parts = chunk.split(":");
                if (parts.length != 3) {
                    System.err.println("Invalid peer format: " + chunk + " (expected nodeId:host:port)");
                    System.exit(1);
                }
                String peerId = parts[0];
                String peerHost = parts[1];
                int peerPort = Integer.parseInt(parts[2]);
                peers.add(new NettyRaftPeer(peerId, peerHost, peerPort, nettyClient));
                logger.info("Added peer: {} at {}:{}", peerId, peerHost, peerPort);
            }
        }

        raftNode.setPeers(peers);

        // 5. KV Request Handler
        KVRequestHandler requestHandler = new KVRequestHandler(raftNode, storageEngine);

        // 6. Netty Server with injected handlers
        Function<KvProtos.KVRequest, KvProtos.KVResponse> kvHandler = requestHandler::handle;
        NettyServer server = new NettyServer(port, raftNode, kvHandler);

        // 7. Metrics HTTP server (binds to port + 1000, e.g. raft=8081 → metrics=9081)
        MetricsServer metricsServer = new MetricsServer(raftNode, port);
        metricsServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Shutting down...");
                metricsServer.stop();
                raftNode.stop();
                server.stop();
                nettyClient.stop();
                storageEngine.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        raftNode.start();

        // No forced becomeLeader() — the election timer will fire and a real election begins.

        server.start();

        // Block until termination
        server.awaitTermination();
    }
}
