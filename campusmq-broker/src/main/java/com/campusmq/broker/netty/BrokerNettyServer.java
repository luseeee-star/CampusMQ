package com.campusmq.broker.netty;

import com.campusmq.broker.BrokerController;
import com.campusmq.common.netty.NettyDecoder;
import com.campusmq.common.netty.NettyEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Broker 网络层：长连接 + 私有二进制协议（执行方案 §1.2）。 */
public class BrokerNettyServer {

    private static final Logger log = LoggerFactory.getLogger(BrokerNettyServer.class);

    private final BrokerController controller;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private ServerHandler serverHandler;

    public BrokerNettyServer(BrokerController controller) {
        this.controller = controller;
    }

    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(3);
        serverHandler = new ServerHandler(controller);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    // 120 秒无读写自动断连，客户端心跳保活
                                    .addLast(new IdleStateHandler(0, 0, 120))
                                    .addLast(new NettyDecoder())
                                    .addLast(new NettyEncoder())
                                    .addLast(serverHandler);
                        }
                    });
            serverChannel = bootstrap.bind(controller.getConfig().getPort()).sync().channel();
            log.info("broker netty server started on port {}", controller.getConfig().getPort());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("bind failed", e);
        }
    }

    public void shutdown() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (serverHandler != null) {
            serverHandler.shutdown();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}
