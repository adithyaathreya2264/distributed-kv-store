package com.dkv.network;

import com.dkv.raft.RaftNode;
import com.dkv.rpc.proto.RpcProtos.RpcMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer {
    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private final int port;
    private final RaftNode raftNode;
    private final java.util.function.Function<com.dkv.kv.proto.KvProtos.KVRequest, com.dkv.kv.proto.KvProtos.KVResponse> kvHandler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    public NettyServer(int port, RaftNode raftNode) {
        this(port, raftNode, null);
    }

    public NettyServer(int port, RaftNode raftNode,
            java.util.function.Function<com.dkv.kv.proto.KvProtos.KVRequest, com.dkv.kv.proto.KvProtos.KVResponse> kvHandler) {
        this.port = port;
        this.raftNode = raftNode;
        this.kvHandler = kvHandler;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                            ch.pipeline().addLast(new ProtobufDecoder(RpcMessage.getDefaultInstance()));
                            ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                            ch.pipeline().addLast(new ProtobufEncoder());
                            ch.pipeline().addLast(new RpcHandler(raftNode, kvHandler));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            channelFuture = b.bind(port).sync();
            logger.info("Netty Server started on port {}", port);

            // Don't block here, let the caller handle lifecycle or just return
        } catch (Exception e) {
            stop();
            throw e;
        }
    }

    public void stop() {
        if (workerGroup != null)
            workerGroup.shutdownGracefully();
        if (bossGroup != null)
            bossGroup.shutdownGracefully();
    }

    public void awaitTermination() throws InterruptedException {
        if (channelFuture != null) {
            channelFuture.channel().closeFuture().sync();
        }
    }
}
