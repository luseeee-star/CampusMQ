package com.campusmq.common.protocol.body;

/** ACK / 心跳等场景的通用响应。 */
public class CommonResponse {
    private boolean ok;
    private String error;

    public static CommonResponse ok() {
        CommonResponse r = new CommonResponse();
        r.ok = true;
        return r;
    }

    public static CommonResponse error(String error) {
        CommonResponse r = new CommonResponse();
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
}
