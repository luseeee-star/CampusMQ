package com.campusmq.broker.store;

/** 写入结果。 */
public class AppendMessageResult {

    public enum Status {
        OK, END_OF_FILE
    }

    private final Status status;
    private final long physicalOffset;
    private final long queueOffset;
    private final int wroteBytes;

    public AppendMessageResult(Status status, long physicalOffset, long queueOffset, int wroteBytes) {
        this.status = status;
        this.physicalOffset = physicalOffset;
        this.queueOffset = queueOffset;
        this.wroteBytes = wroteBytes;
    }

    public static AppendMessageResult ok(long physicalOffset, long queueOffset, int wroteBytes) {
        return new AppendMessageResult(Status.OK, physicalOffset, queueOffset, wroteBytes);
    }

    public Status getStatus() {
        return status;
    }

    public long getPhysicalOffset() {
        return physicalOffset;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }
}
