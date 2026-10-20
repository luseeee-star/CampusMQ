package com.campusmq.client.consumer;

import com.campusmq.client.remoting.RemotingClient;
import com.campusmq.common.protocol.RemotingCommand;
import com.campusmq.common.protocol.RequestCode;
import com.campusmq.common.protocol.body.AckMessageRequest;
import com.campusmq.common.protocol.body.CommonResponse;
import com.campusmq.common.protocol.body.PullMessageRequest;
import com.campusmq.common.protocol.body.PullMessageResponse;

/** 拉模式消费者 SDK。offset 传 -1 表示用 Broker 上该组的持久化进度。 */
public class DefaultMQPullConsumer {

    private final String group;
    private final RemotingClient client;

    public DefaultMQPullConsumer(String group, String brokerAddr) {
        this.group = group;
        String[] parts = brokerAddr.split(":");
        this.client = new RemotingClient(parts[0], Integer.parseInt(parts[1]));
    }

    public void start() {
        client.start();
    }

    public PullResult pull(String topic, int queueId, long offset, int maxNum) {
        PullMessageRequest req = new PullMessageRequest();
        req.setTopic(topic);
        req.setGroup(group);
        req.setQueueId(queueId);
        req.setOffset(offset);
        req.setMaxNum(maxNum);

        RemotingCommand cmd = RemotingCommand.request(RequestCode.PULL_MESSAGE, RemotingClient.nextOpaque(), req);
        RemotingCommand resp = client.sendSync(cmd);
        if (resp == null) {
            throw new IllegalStateException("pull timeout");
        }
        PullMessageResponse body = resp.bodyAs(PullMessageResponse.class);
        return new PullResult(body.getStatus(), body.getNextOffset(), body.getMessages());
    }

    /** 消费成功后提交进度（先业务后进度 = 至少一次语义） */
    public boolean ack(String topic, int queueId, long offset) {
        AckMessageRequest req = new AckMessageRequest();
        req.setTopic(topic);
        req.setGroup(group);
        req.setQueueId(queueId);
        req.setOffset(offset);

        RemotingCommand cmd = RemotingCommand.request(RequestCode.ACK_MESSAGE, RemotingClient.nextOpaque(), req);
        RemotingCommand resp = client.sendSync(cmd);
        return resp != null && resp.bodyAs(CommonResponse.class).isOk();
    }

    public void shutdown() {
        client.shutdown();
    }

    public String getGroup() {
        return group;
    }
}
