package com.dkv.client;

import com.dkv.kv.proto.KvProtos;
import com.dkv.network.NettyClient;
import com.dkv.rpc.proto.RpcProtos.RpcMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class DKVClient {
    private static final Logger logger = LoggerFactory.getLogger(DKVClient.class);
    private final NettyClient nettyClient;
    private final List<String> seedNodes; // "host:port" strings
    private final AtomicInteger currentLeaderIndex = new AtomicInteger(0);

    public DKVClient(List<String> seedNodes) {
        this.nettyClient = new NettyClient();
        this.seedNodes = seedNodes;
    }

    public void close() {
        nettyClient.stop();
    }

    public CompletableFuture<Void> put(String key, String value) {
        return sendRequestWithRetry(
                KvProtos.KVRequest.newBuilder()
                        .setType(KvProtos.KVRequest.CommandType.PUT)
                        .setKey(key)
                        .setValue(value)
                        .build())
                .thenAccept(response -> {
                    if (!response.getSuccess()) {
                        throw new KVClientException("PUT failed: " + response.getMessage());
                    }
                });
    }

    public CompletableFuture<String> get(String key) {
        return sendRequestWithRetry(
                KvProtos.KVRequest.newBuilder()
                        .setType(KvProtos.KVRequest.CommandType.GET)
                        .setKey(key)
                        .build())
                .thenApply(response -> {
                    if (!response.getSuccess()) {
                        // Could be "Key not found" or actual error.
                        // For "Key not found", we might return null or throw.
                        // Let's assume message contains details.
                        if ("Key not found".equals(response.getMessage())) {
                            return null;
                        }
                        throw new KVClientException("GET failed: " + response.getMessage());
                    }
                    return response.getValue();
                });
    }

    public CompletableFuture<Void> delete(String key) {
        return sendRequestWithRetry(
                KvProtos.KVRequest.newBuilder()
                        .setType(KvProtos.KVRequest.CommandType.DELETE)
                        .setKey(key)
                        .build())
                .thenAccept(response -> {
                    if (!response.getSuccess()) {
                        throw new KVClientException("DELETE failed: " + response.getMessage());
                    }
                });
    }

    private CompletableFuture<KvProtos.KVResponse> sendRequestWithRetry(KvProtos.KVRequest kvRequest) {
        return attemptRequest(kvRequest, 0);
    }

    private CompletableFuture<KvProtos.KVResponse> attemptRequest(KvProtos.KVRequest kvRequest, int attempt) {
        if (attempt >= seedNodes.size() * 2) {
            return CompletableFuture.failedFuture(new KVClientException("Max retries exceeded"));
        }

        String address = seedNodes.get(currentLeaderIndex.get() % seedNodes.size());
        String[] parts = address.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        RpcMessage rpcMessage = RpcMessage.newBuilder()
                .setType(RpcMessage.MessageType.KV_REQUEST)
                .setRequestId("req-" + java.util.UUID.randomUUID())
                .setKvRequest(kvRequest)
                .build();

        logger.info("Sending {} to {}", kvRequest.getType(), address);

        return nettyClient.sendRequest(host, port, rpcMessage)
                .handle((response, ex) -> {
                    if (ex != null) {
                        logger.warn("Request to {} failed: {}", address, ex.getMessage());
                        // Network error, try next node
                        rotateLeader();
                        return attemptRequest(kvRequest, attempt + 1);
                    }

                    if (response.getType() == RpcMessage.MessageType.KV_RESPONSE) {
                        KvProtos.KVResponse kvResponse = response.getKvResponse();
                        if (!kvResponse.getSuccess() && "Not Leader".equals(kvResponse.getMessage())) {
                            logger.info("Node {} is not leader, retrying...", address);
                            rotateLeader();
                            return attemptRequest(kvRequest, attempt + 1);
                        }
                        return CompletableFuture.completedFuture(kvResponse);
                    } else {
                        return CompletableFuture.<KvProtos.KVResponse>failedFuture(
                                new KVClientException("Invalid response type: " + response.getType()));
                    }
                })
                .thenCompose(future -> future); // Flatten the nested future
    }

    private void rotateLeader() {
        currentLeaderIndex.incrementAndGet();
    }
}
