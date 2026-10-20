package com.campusmq.client.producer;

/** 发送结果。messageId 是业务幂等去重键。 */
public class SendResult {
    private final boolean ok;
    private final String error;
    private final String messageId;
    private final long queueOffset;

    public SendResult(boolean ok, String error, String messageId, long queueOffset) {
        this.ok = ok;
        this.error = error;
        this.messageId = messageId;
        this.queueOffset = queueOffset;
    }

    public boolean isOk() {
        return ok;
    }

    public String getError() {
        return error;
    }

    public String getMessageId() {
        return messageId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    @Override
    public String toString() {
        return ok ? "SendResult{ok, messageId=" + messageId + ", queueOffset=" + queueOffset + "}"
                : "SendResult{error=" + error + "}";
    }
}
