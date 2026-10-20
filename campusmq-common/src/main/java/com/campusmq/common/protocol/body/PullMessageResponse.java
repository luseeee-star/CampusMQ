package com.campusmq.common.protocol.body;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** code=11 的响应体。 */
public class PullMessageResponse {

    public static final int STATUS_FOUND = 0;
    public static final int STATUS_NO_NEW_MSG = 1;
    public static final int STATUS_OFFSET_ILLEGAL = 2;

    private int status;
    /** 下一应从该位点继续拉 */
    private long nextOffset;
    private List<Item> messages = new ArrayList<>();

    public static class Item {
        private String messageId;
        private long queueOffset;
        private long deliverTimestamp;
        private int reconsumeTimes;
        private Map<String, String> properties;
        private byte[] body;

        public String getMessageId() {
            return messageId;
        }

        public void setMessageId(String messageId) {
            this.messageId = messageId;
        }

        public long getQueueOffset() {
            return queueOffset;
        }

        public void setQueueOffset(long queueOffset) {
            this.queueOffset = queueOffset;
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

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public List<Item> getMessages() {
        return messages;
    }

    public void setMessages(List<Item> messages) {
        this.messages = messages;
    }
}
