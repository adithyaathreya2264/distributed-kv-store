package com.dkv.network;

import com.dkv.rpc.proto.RpcProtos.RpcMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class NettyClient {
    private final EventLoopGroup group;
    // Simple connection cache: "host:port" -> Channel
    private final Map<String, Channel> channelCache = new ConcurrentHashMap<>();

    // Request tracking: requestId -> CompletableFuture
    private final Map<String, CompletableFuture<RpcMessage>> pendingRequests = new ConcurrentHashMap<>();

    public NettyClient() {
        this.group = new NioEventLoopGroup();
    }

    private Channel getChannel(String host, int port) throws InterruptedException {
        String key = host + ":" + port;
        if (channelCache.containsKey(key)) {
            Channel ch = channelCache.get(key);
            if (ch.isActive()) {
                return ch;
            }
        }

        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                        ch.pipeline().addLast(new ProtobufDecoder(RpcMessage.getDefaultInstance()));
                        ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                        ch.pipeline().addLast(new ProtobufEncoder());
                        ch.pipeline().addLast(new ClientResponseHandler());
                    }
                });

        Channel ch = b.connect(host, port).sync().channel();
        channelCache.put(key, ch);
        return ch;
    }

    public CompletableFuture<RpcMessage> sendRequest(String host, int port, RpcMessage request) {
        CompletableFuture<RpcMessage> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), future);

        try {
            Channel ch = getChannel(host, port);
            ch.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                if (!f.isSuccess()) {
                    pendingRequests.remove(request.getRequestId());
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            pendingRequests.remove(request.getRequestId());
            future.completeExceptionally(e);
        }
        return future;
    }

    public void stop() {
        group.shutdownGracefully();
    }

    private class ClientResponseHandler extends SimpleChannelInboundHandler<RpcMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
            CompletableFuture<RpcMessage> future = pendingRequests.remove(msg.getRequestId());
            if (future != null) {
                future.complete(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
            // In a real system, we'd fail all pending requests for this channel
        }
    }
}
