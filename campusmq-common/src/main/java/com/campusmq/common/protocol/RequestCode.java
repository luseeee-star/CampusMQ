package com.campusmq.common.protocol;

/**
 * 请求码（执行方案 §4.2）。
 * 响应沿用同一 code，通过 opaque 配对。
 */
public final class RequestCode {
    public static final int SEND_MESSAGE = 10;
    public static final int PULL_MESSAGE = 11;
    public static final int ACK_MESSAGE = 12;
    public static final int HEARTBEAT = 13;

    private RequestCode() {
    }
}
