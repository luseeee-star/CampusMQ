package com.campusslot.app.gateway;

import jakarta.servlet.http.HttpServletRequest;

/**
 * 当前请求的身份。统一身份认证（CAS/OAuth2）接入前的过渡实现是从请求头取学号，
 * 接入后由 CAS 过滤器写入，业务代码不感知差异。
 */
public final class StudentContext {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    public static final String ATTR_STUDENT_ID = "campusslot.studentId";
    public static final String HEADER_STUDENT_ID = "X-Student-Id";

    private StudentContext() {
    }

    public static void set(String studentId) {
        CURRENT.set(studentId);
    }

    public static String get() {
        return CURRENT.get();
    }

    public static String require() {
        String studentId = CURRENT.get();
        if (studentId == null || studentId.isBlank()) {
            throw new com.campusslot.common.api.BizException(
                    com.campusslot.common.api.ErrorCode.STUDENT_NOT_LOGIN);
        }
        return studentId;
    }

    public static void clear() {
        CURRENT.remove();
    }

    /** 优先取 Filter 已解析的值，未经过 Filter 时回落到请求头。 */
    public static String resolve(HttpServletRequest request) {
        Object attr = request.getAttribute(ATTR_STUDENT_ID);
        if (attr instanceof String s && !s.isBlank()) {
            return s;
        }
        String header = request.getHeader(HEADER_STUDENT_ID);
        return header == null || header.isBlank() ? null : header.trim();
    }
}
