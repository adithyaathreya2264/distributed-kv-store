package com.dkv.network;

import com.dkv.raft.RaftNode;
import com.dkv.rpc.proto.RpcProtos.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcHandler extends SimpleChannelInboundHandler<RpcMessage> {
    private static final Logger logger = LoggerFactory.getLogger(RpcHandler.class);
    private final RaftNode raftNode;
    private final java.util.function.Function<com.dkv.kv.proto.KvProtos.KVRequest, com.dkv.kv.proto.KvProtos.KVResponse> kvHandler;

    public RpcHandler(RaftNode raftNode) {
        this(raftNode, null);
    }

    public RpcHandler(RaftNode raftNode,
            java.util.function.Function<com.dkv.kv.proto.KvProtos.KVRequest, com.dkv.kv.proto.KvProtos.KVResponse> kvHandler) {
        this.raftNode = raftNode;
        this.kvHandler = kvHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
        RpcMessage response = null;

        switch (msg.getType()) {
            case REQUEST_VOTE_REQUEST:
                var rvResponse = raftNode.handleRequestVote(msg.getRequestVoteRequest());
                response = RpcMessage.newBuilder()
                        .setType(RpcMessage.MessageType.REQUEST_VOTE_RESPONSE)
                        .setRequestId(msg.getRequestId())
                        .setRequestVoteResponse(rvResponse)
                        .build();
                break;

            case APPEND_ENTRIES_REQUEST:
                var aeResponse = raftNode.handleAppendEntries(msg.getAppendEntriesRequest());
                response = RpcMessage.newBuilder()
                        .setType(RpcMessage.MessageType.APPEND_ENTRIES_RESPONSE)
                        .setRequestId(msg.getRequestId())
                        .setAppendEntriesResponse(aeResponse)
                        .build();
                break;

            case KV_REQUEST:
                if (kvHandler != null) {
                    var kvResponse = kvHandler.apply(msg.getKvRequest());
                    response = RpcMessage.newBuilder()
                            .setType(RpcMessage.MessageType.KV_RESPONSE)
                            .setRequestId(msg.getRequestId())
                            .setKvResponse(kvResponse)
                            .build();
                } else {
                    logger.warn("KV Handler not configured");
                }
                break;

            case INSTALL_SNAPSHOT_REQUEST:
                var isResponse = raftNode.handleInstallSnapshot(msg.getInstallSnapshotRequest());
                response = RpcMessage.newBuilder()
                        .setType(RpcMessage.MessageType.INSTALL_SNAPSHOT_RESPONSE)
                        .setRequestId(msg.getRequestId())
                        .setInstallSnapshotResponse(isResponse)
                        .build();
                break;

            default:
                logger.warn("Received unsupported or response message type: {}", msg.getType());
        }

        if (response != null) {
            ctx.writeAndFlush(response);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("RPC Handler exception", cause);
        ctx.close();
    }
}
