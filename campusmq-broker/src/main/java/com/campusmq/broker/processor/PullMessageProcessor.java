package com.campusmq.broker.processor;

import com.campusmq.broker.BrokerController;
import com.campusmq.common.message.MessageExt;
import com.campusmq.common.protocol.RemotingCommand;
import com.campusmq.common.protocol.body.PullMessageRequest;
import com.campusmq.common.protocol.body.PullMessageResponse;

import java.util.List;

/** 拉取请求处理：offset=-1 时用该消费组的持久化进度。 */
public class PullMessageProcessor {

    private final BrokerController brokerController;

    public PullMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public RemotingCommand process(RemotingCommand request) {
        PullMessageRequest req = request.bodyAs(PullMessageRequest.class);
        long offset = req.getOffset();
        if (offset < 0) {
            offset = brokerController.getOffsetManager()
                    .queryOffset(req.getTopic(), req.getGroup(), req.getQueueId());
        }

        List<MessageExt> msgs = brokerController.getMessageStore()
                .getMessage(req.getTopic(), req.getQueueId(), offset, req.getMaxNum());

        PullMessageResponse resp = new PullMessageResponse();
        if (msgs.isEmpty()) {
            resp.setStatus(PullMessageResponse.STATUS_NO_NEW_MSG);
            resp.setNextOffset(offset);
        } else {
            resp.setStatus(PullMessageResponse.STATUS_FOUND);
            resp.setNextOffset(msgs.get(msgs.size() - 1).getQueueOffset() + 1);
            String host = brokerController.getConfig().getHost();
            int port = brokerController.getConfig().getPort();
            for (MessageExt m : msgs) {
                PullMessageResponse.Item item = new PullMessageResponse.Item();
                item.setMessageId(m.messageId(host, port));
                item.setQueueOffset(m.getQueueOffset());
                item.setDeliverTimestamp(m.getDeliverTimestamp());
                item.setReconsumeTimes(m.getReconsumeTimes());
                item.setProperties(m.getProperties());
                item.setBody(m.getBody());
                resp.getMessages().add(item);
            }
        }
        return RemotingCommand.response(request, resp);
    }
}
