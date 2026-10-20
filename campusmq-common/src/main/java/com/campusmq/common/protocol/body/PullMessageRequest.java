package com.campusmq.common.protocol.body;

/** code=11 的请求体。 */
public class PullMessageRequest {
    private String topic;
    private String group;
    private int queueId;
    /** 期望从哪个逻辑位点开始拉；-1 表示用该组的消费进度 */
    private long offset;
    private int maxNum = 32;

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

    public int getMaxNum() {
        return maxNum;
    }

    public void setMaxNum(int maxNum) {
        this.maxNum = maxNum;
    }
}
