package com.campusslot.common.api;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 全局异常处理：保证任何失败都有明确原因码，不做静默失败（需求书 §4.1 P0-4）。 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(BizException.class)
    public R<Void> handleBiz(BizException e, HttpServletRequest request) {
        // 主动拒绝走 warn 级，避免污染 5xx 统计
        if (e.getErrorCode().isActiveRejection()) {
            log.warn("biz reject path={} code={} msg={}", request.getRequestURI(),
                    e.getErrorCode().getCode(), e.getMessage());
        } else {
            log.info("biz error path={} code={} msg={}", request.getRequestURI(),
                    e.getErrorCode().getCode(), e.getMessage());
        }
        return R.fail(e.getErrorCode(), e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public R<Void> handleUnknown(Exception e, HttpServletRequest request) {
        log.error("unhandled error path={}", request.getRequestURI(), e);
        return R.fail(ErrorCode.SYSTEM_BUSY);
    }
}
