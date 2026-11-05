package com.campusslot.app.claim;

import com.campusslot.app.gateway.StudentContext;
import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.api.R;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 取消入口（S-4）：{@code POST /api/claim/cancel}。
 *
 * <p>与确认同构（{@code POST /api/claim/confirm}）：都用 {@code (resourceId, studentId)}
 * 定位，都不带 recordId，都一律返回 HTTP 200 用业务码区分。这样前端两个按钮
 * 共用同一套解析逻辑，压测脚本也只需要换 path。
 *
 * <p><b>为什么路径是 {@code /api/claim/cancel} 而不是 {@code /api/claim/{resourceId}/cancel}：
 * </b>{@code RateLimitFilter} 的 {@code include-paths} 是 {@code AntPathMatcher} <b>精确匹配</b>
 * {@code /api/claim} 与 {@code /api/claim/mq}，带 id 的路径本来就进不了桶——两种写法都不会被限流。
 * 选不带 id 的写法是为了与 confirm 完全对称，让"可靠路径两个动作形状一致"成为可读的事实，
 * 而不是靠一条"带 id 的恰好不匹配"的隐式性质。
 *
 * <p><b>HTTP 状态码同样一律 200。</b>取消一条已确认的记录是业务码 2004（状态互斥），
 * 重复取消是 200 幂等（带 {@code X-Cancel-Replay: 1}），过截止时间是 2005。
 * 前端不需要为取消单独写一套状态码解析。
 */
@RestController
@RequestMapping("/api/claim")
public class CancelController {

    /** 与 {@code X-Confirm-Replay} 同构：前端据此提示"你之前已经取消过了"。 */
    static final String HEADER_REPLAY = "X-Cancel-Replay";

    private final ReleaseService releaseService;

    public CancelController(ReleaseService releaseService) {
        this.releaseService = releaseService;
    }

    @PostMapping("/cancel")
    public R<Map<String, Object>> cancel(@RequestParam long resourceId,
                                         @RequestParam(required = false) String studentId,
                                         HttpServletResponse response) {
        // 身份优先取上下文，参数只作回落：与 confirm 完全一致，
        // 接入统一认证后学生无法通过改参数取消别人的名额。
        String resolved = StudentContext.get();
        if (resolved == null || resolved.isBlank()) {
            resolved = studentId;
        }
        if (resolved == null || resolved.isBlank()) {
            return R.fail(ErrorCode.STUDENT_NOT_LOGIN);
        }

        ReleaseOutcome outcome = releaseService.cancel(resourceId, resolved);
        if (!outcome.success()) {
            String message = outcome.note() == null ? outcome.error().getMessage() : outcome.note();
            return R.fail(outcome.error(), message);
        }

        if (outcome.replayed()) {
            response.setHeader(HEADER_REPLAY, "1");
        }
        // 余量已在 Redis 对齐时 synced=true；false 表示走了对账兜底（用户侧仍是成功）。
        // 这个标记只进响应头，不进响应体：前端无法处理它，也不该处理。
        if (!outcome.synced()) {
            response.setHeader("X-Cancel-Pending-Reconcile", "1");
        }

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("resourceId", resourceId);
        data.put("status", outcome.status());
        data.put("releaseReason", outcome.releaseReason());
        data.put("remain", outcome.remain());
        return R.ok(data);
    }
}
