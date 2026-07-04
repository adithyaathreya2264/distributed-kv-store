package com.dkv.server;

import com.dkv.raft.RaftNode;
import com.dkv.raft.RaftPeer;
import com.dkv.raft.RaftState;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Lightweight HTTP server that exposes a /metrics endpoint using the built-in
 * JDK HttpServer. No external framework needed.
 *
 * Binds to raftPort + 1000 (e.g. raft=8081 → metrics=9081).
 * Returns a JSON object with the node's current Raft state.
 */
public class MetricsServer {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServer.class);

    private final RaftNode raftNode;
    private final int metricsPort;
    private HttpServer server;

    public MetricsServer(RaftNode raftNode, int raftPort) {
        this.raftNode = raftNode;
        this.metricsPort = raftPort + 1000;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(metricsPort), 0);
        server.createContext("/metrics", this::handleMetrics);
        server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "metrics-http");
            t.setDaemon(true);
            return t;
        }));
        server.start();
        logger.info("Metrics server started on http://localhost:{}/metrics", metricsPort);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        // Allow any origin so the static dashboard.html (opened from file://) can fetch
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");

        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS");
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        String body = buildMetricsJson();
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String buildMetricsJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"nodeId\": \"").append(raftNode.getNodeId()).append("\",\n");
        sb.append("  \"state\": \"").append(raftNode.getState().name()).append("\",\n");
        sb.append("  \"term\": ").append(raftNode.getCurrentTerm()).append(",\n");
        sb.append("  \"commitIndex\": ").append(raftNode.getCommitIndex()).append(",\n");
        sb.append("  \"lastApplied\": ").append(raftNode.getLastApplied()).append(",\n");

        List<RaftPeer> peers = raftNode.getPeers();
        sb.append("  \"peers\": [\n");
        for (int i = 0; i < peers.size(); i++) {
            RaftPeer p = peers.get(i);
            boolean isLeader = raftNode.getState() == RaftState.LEADER;
            sb.append("    {\n");
            sb.append("      \"peerId\": \"").append(p.getId()).append("\",\n");
            sb.append("      \"nextIndex\": ").append(isLeader ? p.getNextIndex() : -1).append(",\n");
            sb.append("      \"matchIndex\": ").append(isLeader ? p.getMatchIndex() : -1).append(",\n");
            sb.append("      \"reachable\": ").append(p.isReachable()).append("\n");
            sb.append("    }");
            if (i < peers.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]\n");
        sb.append("}");
        return sb.toString();
    }
}
