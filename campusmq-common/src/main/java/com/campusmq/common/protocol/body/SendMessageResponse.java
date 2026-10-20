package com.campusmq.common.protocol.body;

/** code=10 的响应体。 */
public class SendMessageResponse {
    private boolean ok;
    private String error;
    /** brokerHost:port:physicalOffset，业务侧幂等去重键 */
    private String messageId;
    private long physicalOffset;
    private long queueOffset;

    public static SendMessageResponse ok(String messageId, long physicalOffset, long queueOffset) {
        SendMessageResponse r = new SendMessageResponse();
        r.ok = true;
        r.messageId = messageId;
        r.physicalOffset = physicalOffset;
        r.queueOffset = queueOffset;
        return r;
    }

    public static SendMessageResponse error(String error) {
        SendMessageResponse r = new SendMessageResponse();
        r.ok = false;
        r.error = error;
        return r;
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

    public long getPhysicalOffset() {
        return physicalOffset;
    }

    public long getQueueOffset() {
        return queueOffset;
    }
}
