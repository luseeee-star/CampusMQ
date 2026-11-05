package com.campusslot.app.gateway.filter;

import com.campusslot.app.gateway.StudentContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * 身份解析 + traceId 落地（执行方案 §5.3：每条跨链路日志必须带 traceId 与 resourceId）。
 * 真实部署时由 CAS/OAuth2 过滤器写入身份，本过滤器保留为统一出口。
 */
@Component
@Order(1)
public class StudentContextFilter extends OncePerRequestFilter {

    public static final String TRACE_ID_HEADER = "X-Trace-Id";
    public static final String ATTR_TRACE_ID = "campusslot.traceId";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String traceId = request.getHeader(TRACE_ID_HEADER);
        if (traceId == null || traceId.isBlank()) {
            traceId = java.util.UUID.randomUUID().toString().replace("-", "");
        }
        response.setHeader(TRACE_ID_HEADER, traceId);

        String studentId = StudentContext.resolve(request);
        if (studentId != null) {
            request.setAttribute(StudentContext.ATTR_STUDENT_ID, studentId);
            StudentContext.set(studentId);
        }
        // MDC 埋点：日志里能按 traceId 串起「网关 → 申领 → 消费者」
        org.slf4j.MDC.put("traceId", traceId);
        org.slf4j.MDC.put("studentId", studentId == null ? "-" : studentId);

        try {
            request.setAttribute(ATTR_TRACE_ID, traceId);
            chain.doFilter(request, response);
        } finally {
            StudentContext.clear();
            org.slf4j.MDC.clear();
        }
    }
}
