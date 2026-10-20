package com.campusmq.client.remoting;

import com.campusmq.common.netty.NettyDecoder;
import com.campusmq.common.netty.NettyEncoder;
import com.campusmq.common.protocol.OpaqueGenerator;
import com.campusmq.common.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 客户端网络层：单长连接，同步发送语义。 */
public class RemotingClient {

    private static final Logger log = LoggerFactory.getLogger(RemotingClient.class);

    private final String host;
    private final int port;
    private final Map<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>();
    private NioEventLoopGroup group;
    private Channel channel;

    public RemotingClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() {
        group = new NioEventLoopGroup(1);
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new NettyDecoder())
                                .addLast(new NettyEncoder())
                                .addLast(new ClientHandler());
                    }
                });
        try {
            channel = bootstrap.connect(host, port).sync().channel();
            log.info("connected to broker {}:{}", host, port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("connect to broker failed", e);
        }
    }

    /** 同步发送：超时返回 null，由调用方决定重试策略 */
    public RemotingCommand sendSync(RemotingCommand cmd, long timeoutMs) {
        ResponseFuture future = new ResponseFuture(cmd.getOpaque());
        responseTable.put(cmd.getOpaque(), future);
        try {
            channel.writeAndFlush(cmd).sync();
            return future.await(timeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("send interrupted", e);
        } finally {
            responseTable.remove(cmd.getOpaque());
        }
    }

    public RemotingCommand sendSync(RemotingCommand cmd) {
        return sendSync(cmd, 5000);
    }

    public static int nextOpaque() {
        return OpaqueGenerator.next();
    }

    public void shutdown() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    private class ClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand cmd) {
            ResponseFuture future = responseTable.get(cmd.getOpaque());
            if (future != null) {
                future.complete(cmd);
            } else {
                log.warn("response without waiter, opaque={}", cmd.getOpaque());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("client channel exception: {}", cause.getMessage());
            ctx.close();
        }
    }
}
