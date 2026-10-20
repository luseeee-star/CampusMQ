package com.campusmq.common.protocol.body;

/** code=12 的请求体。offset 为消费成功后推进到的位点。 */
public class AckMessageRequest {
    private String topic;
    private String group;
    private int queueId;
    private long offset;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
