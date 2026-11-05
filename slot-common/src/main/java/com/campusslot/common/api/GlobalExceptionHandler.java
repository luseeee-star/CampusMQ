package com.campusslot.common.api;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
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

    /**
     * 请求体字段校验失败（{@code @Valid} + {@code @NotBlank} 这类）。
     *
     * <p><b>不接这个异常会变成 500 SYSTEM_BUSY</b>：Spring 抛的
     * {@code MethodArgumentNotValidException} 会落到下面的兜底分支，于是"少传一个 deptId"
     * 对外表现为"系统繁忙，请稍后重试"——调用方会重试，而重试永远不会成功。
     * 这类假故障在演示现场最难解释，因为日志里只有一行 unhandled error。
     *
     * <p>只回第一条错误：一次回全部字段错误，前端反而不知道该先改哪个；
     * 而且我们自己的校验（如名单第几行缺学院编码）本来就是逐条报的。
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public R<Void> handleFieldInvalid(MethodArgumentNotValidException e, HttpServletRequest request) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(GlobalExceptionHandler::describe)
                .orElse(ErrorCode.PARAM_INVALID.getMessage());
        log.info("param invalid path={} msg={}", request.getRequestURI(), message);
        return R.fail(ErrorCode.PARAM_INVALID, message);
    }

    /**
     * 请求体根本读不出来：JSON 语法错、时间格式不是 ISO、字段类型不匹配。
     *
     * <p>同样不能落到 500。最常见的是时间写成 {@code 2026-11-10 09:00:00}
     * （空格分隔），Jackson 默认只认 {@code T} 分隔；返回"系统繁忙"会让人去查数据库。
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public R<Void> handleUnreadable(HttpMessageNotReadableException e, HttpServletRequest request) {
        log.info("body unreadable path={} reason={}", request.getRequestURI(), e.getMostSpecificCause().getMessage());
        return R.fail(ErrorCode.PARAM_INVALID,
                "请求体无法解析：检查 JSON 语法与时间格式（ISO，如 2026-11-10T09:00:00）");
    }

    private static String describe(FieldError error) {
        String reason = error.getDefaultMessage();
        return (error.getField() == null || error.getField().isBlank())
                ? reason : error.getField() + "：" + reason;
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public R<Void> handleUnknown(Exception e, HttpServletRequest request) {
        log.error("unhandled error path={}", request.getRequestURI(), e);
        return R.fail(ErrorCode.SYSTEM_BUSY);
    }
}
