package com.campusmq.client.producer;

import com.campusmq.client.remoting.RemotingClient;
import com.campusmq.common.protocol.RemotingCommand;
import com.campusmq.common.protocol.RequestCode;
import com.campusmq.common.protocol.body.SendMessageRequest;
import com.campusmq.common.protocol.body.SendMessageResponse;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/** 生产者 SDK。 */
public class DefaultMQProducer {

    private final RemotingClient client;
    private final int defaultQueueId = 0;

    public DefaultMQProducer(String brokerAddr) {
        String[] parts = brokerAddr.split(":");
        this.client = new RemotingClient(parts[0], Integer.parseInt(parts[1]));
    }

    public void start() {
        client.start();
    }

    public SendResult send(String topic, String body) {
        return send(topic, body.getBytes(StandardCharsets.UTF_8), 0, null);
    }

    public SendResult send(String topic, byte[] body, long delayTimeMs, Map<String, String> properties) {
        SendMessageRequest req = new SendMessageRequest();
        req.setTopic(topic);
        req.setQueueId(defaultQueueId);
        req.setBody(body);
        req.setDelayTimeMs(delayTimeMs);
        req.setProperties(properties);

        RemotingCommand cmd = RemotingCommand.request(RequestCode.SEND_MESSAGE, RemotingClient.nextOpaque(), req);
        RemotingCommand resp = client.sendSync(cmd);
        if (resp == null) {
            return new SendResult(false, "send timeout", null, -1);
        }
        SendMessageResponse body1 = resp.bodyAs(SendMessageResponse.class);
        return new SendResult(body1.isOk(), body1.getError(), body1.getMessageId(), body1.getQueueOffset());
    }

    public void shutdown() {
        client.shutdown();
    }
}
