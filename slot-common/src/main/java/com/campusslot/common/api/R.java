package com.campusslot.common.api;

/** 统一响应体。code=0 表示成功，非 0 为业务错误码（见 {@link ErrorCode}）。 */
public record R<T>(int code, String message, T data, String traceId) {

    public static <T> R<T> ok(T data) {
        return new R<>(0, "ok", data, null);
    }

    public static R<Void> ok() {
        return new R<>(0, "ok", null, null);
    }

    public static <T> R<T> fail(ErrorCode errorCode) {
        return new R<>(errorCode.getCode(), errorCode.getMessage(), null, null);
    }

    public static <T> R<T> fail(ErrorCode errorCode, String message) {
        return new R<>(errorCode.getCode(), message, null, null);
    }

    public boolean isSuccess() {
        return code == 0;
    }
}
