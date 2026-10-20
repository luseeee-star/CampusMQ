package com.campusmq.client.consumer;

import com.campusmq.common.protocol.body.PullMessageResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** 一次拉取的结果。 */
public class PullResult {

    private final int status;
    private final long nextOffset;
    private final List<PullMessageResponse.Item> messages;

    public PullResult(int status, long nextOffset, List<PullMessageResponse.Item> messages) {
        this.status = status;
        this.nextOffset = nextOffset;
        this.messages = messages;
    }

    public boolean found() {
        return status == PullMessageResponse.STATUS_FOUND;
    }

    public int getStatus() {
        return status;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public List<PullMessageResponse.Item> getMessages() {
        return messages;
    }

    public static String bodyAsString(PullMessageResponse.Item item) {
        return item.getBody() == null ? "" : new String(item.getBody(), StandardCharsets.UTF_8);
    }
}
