package com.campusmq.broker.processor;

import com.campusmq.broker.BrokerController;
import com.campusmq.broker.store.AppendMessageResult;
import com.campusmq.common.message.MessageExt;
import com.campusmq.common.protocol.RemotingCommand;
import com.campusmq.common.protocol.body.SendMessageRequest;
import com.campusmq.common.protocol.body.SendMessageResponse;

import java.util.HashMap;

/** 写入请求处理：解码 -> 落 CommitLog ->（同步策略下刷盘后）返回 messageId。 */
public class SendMessageProcessor {

    private final BrokerController brokerController;

    public SendMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public RemotingCommand process(RemotingCommand request) {
        try {
            SendMessageRequest req = request.bodyAs(SendMessageRequest.class);
            MessageExt msg = new MessageExt();
            msg.setTopic(req.getTopic());
            msg.setQueueId(req.getQueueId());
            msg.setBody(req.getBody());
            msg.setProperties(req.getProperties() == null ? new HashMap<>() : req.getProperties());
            if (req.getDelayTimeMs() > 0) {
                // D7 才接 DelayLog；当前直接设置投递时刻，reput 侧按时间过滤可见性
                msg.setDeliverTimestamp(System.currentTimeMillis() + req.getDelayTimeMs());
            }

            AppendMessageResult result = brokerController.getMessageStore().putMessage(msg);
            String messageId = brokerController.getConfig().getHost() + ":"
                    + brokerController.getConfig().getPort() + ":" + result.getPhysicalOffset();
            return RemotingCommand.response(request,
                    SendMessageResponse.ok(messageId, result.getPhysicalOffset(), result.getQueueOffset()));
        } catch (Exception e) {
            return RemotingCommand.response(request, SendMessageResponse.error(e.getMessage()));
        }
    }
}
