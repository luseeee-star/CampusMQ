package com.campusmq.broker.netty;

import com.campusmq.broker.BrokerController;
import com.campusmq.broker.processor.AckProcessor;
import com.campusmq.broker.processor.PullMessageProcessor;
import com.campusmq.broker.processor.SendMessageProcessor;
import com.campusmq.common.protocol.RemotingCommand;
import com.campusmq.common.protocol.RequestCode;
import com.campusmq.common.protocol.body.CommonResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** 按请求码分发到处理器，业务执行走独立线程池，不占 IO 线程。无连接级状态，可安全共享。 */
@io.netty.channel.ChannelHandler.Sharable
public class ServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    private static final Logger log = LoggerFactory.getLogger(ServerHandler.class);

    private final SendMessageProcessor sendProcessor;
    private final PullMessageProcessor pullProcessor;
    private final AckProcessor ackProcessor;
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "broker-processor");
        t.setDaemon(true);
        return t;
    });

    public ServerHandler(BrokerController controller) {
        this.sendProcessor = new SendMessageProcessor(controller);
        this.pullProcessor = new PullMessageProcessor(controller);
        this.ackProcessor = new AckProcessor(controller);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand cmd) {
        executor.execute(() -> {
            RemotingCommand response;
            try {
                switch (cmd.getCode()) {
                    case RequestCode.SEND_MESSAGE -> response = sendProcessor.process(cmd);
                    case RequestCode.PULL_MESSAGE -> response = pullProcessor.process(cmd);
                    case RequestCode.ACK_MESSAGE -> response = ackProcessor.process(cmd);
                    case RequestCode.HEARTBEAT -> response = RemotingCommand.response(cmd, CommonResponse.ok());
                    default -> response = RemotingCommand.response(cmd,
                            CommonResponse.error("unknown code " + cmd.getCode()));
                }
            } catch (Exception e) {
                log.error("process request failed, code={}", cmd.getCode(), e);
                response = RemotingCommand.response(cmd, CommonResponse.error(e.getMessage()));
            }
            ctx.writeAndFlush(response);
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("channel exception, close: {}", cause.getMessage());
        ctx.close();
    }

    public void shutdown() {
        executor.shutdown();
    }
}
