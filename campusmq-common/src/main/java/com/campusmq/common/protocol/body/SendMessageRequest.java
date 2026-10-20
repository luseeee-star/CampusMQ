package com.campusmq.common.protocol.body;

import java.util.Map;

/** code=10 的请求体。byte[] 由 Jackson 自动转 Base64。 */
public class SendMessageRequest {
    private String topic;
    private int queueId;
    private byte[] body;
    /** 延迟毫秒数，0 表示立即投递（D7 才启用延迟链路） */
    private long delayTimeMs;
    private Map<String, String> properties;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getDelayTimeMs() {
        return delayTimeMs;
    }

    public void setDelayTimeMs(long delayTimeMs) {
        this.delayTimeMs = delayTimeMs;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
