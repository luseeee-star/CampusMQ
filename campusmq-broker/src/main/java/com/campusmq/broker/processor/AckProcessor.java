package com.campusmq.broker.processor;

import com.campusmq.broker.BrokerController;
import com.campusmq.common.protocol.RemotingCommand;
import com.campusmq.common.protocol.body.AckMessageRequest;
import com.campusmq.common.protocol.body.CommonResponse;

/** 消费确认：推进该组的消费进度。重试/死信在 D6 接入，这里只做进度提交。 */
public class AckProcessor {

    private final BrokerController brokerController;

    public AckProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public RemotingCommand process(RemotingCommand request) {
        AckMessageRequest req = request.bodyAs(AckMessageRequest.class);
        brokerController.getOffsetManager()
                .commitOffset(req.getTopic(), req.getGroup(), req.getQueueId(), req.getOffset());
        return RemotingCommand.response(request, CommonResponse.ok());
    }
}
