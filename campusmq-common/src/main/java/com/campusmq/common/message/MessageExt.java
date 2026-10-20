package com.campusmq.common.message;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * CommitLog 中存储的一条消息（执行方案 §3.2）。
 * messageId 不落盘，由 brokerHost:port:physicalOffset 组合生成。
 */
public class MessageExt {

    private String topic;
    private int queueId;
    private long queueOffset;
    private long physicalOffset;
    private int flag;
    private long storeTimestamp;
    /** 可投递时刻；非延迟消息等于 storeTimestamp */
    private long deliverTimestamp;
    private int reconsumeTimes;
    private Map<String, String> properties = new HashMap<>();
    private byte[] body;

    /** properties 序列化：k1|v1|k2|v2（需求 §3.2，竖线分隔键值对） */
    public String propertiesString() {
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        properties.forEach((k, v) -> {
            if (sb.length() > 0) {
                sb.append('|');
            }
            sb.append(k).append('|').append(v);
        });
        return sb.toString();
    }

    public static Map<String, String> stringToProperties(String s) {
        Map<String, String> map = new HashMap<>();
        if (s == null || s.isEmpty()) {
            return map;
        }
        String[] parts = s.split("\\|", -1);
        for (int i = 0; i + 1 < parts.length; i += 2) {
            map.put(parts[i], parts[i + 1]);
        }
        return map;
    }

    public String bodyString() {
        return body == null ? "" : new String(body, StandardCharsets.UTF_8);
    }

    public String messageId(String brokerHost, int brokerPort) {
        return brokerHost + ":" + brokerPort + ":" + physicalOffset;
    }

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

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getPhysicalOffset() {
        return physicalOffset;
    }

    public void setPhysicalOffset(long physicalOffset) {
        this.physicalOffset = physicalOffset;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getDeliverTimestamp() {
        return deliverTimestamp;
    }

    public void setDeliverTimestamp(long deliverTimestamp) {
        this.deliverTimestamp = deliverTimestamp;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
