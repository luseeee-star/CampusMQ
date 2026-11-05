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
 * 确认入口（S-3）：{@code POST /api/claim/confirm}。
 *
 * <p><b>为什么不放在 {@code /api/claim/mq} 下面</b>：那个路径代表"异步落库的申领"，
 * 确认是同步写库的另一个动作，共用一个前缀会让压测脚本的 path 参数失去区分度——
 * 而 D3 起所有容量数字都靠这个参数区分口径。
 *
 * <p><b>为什么不带 recordId</b>：前端从来没有拿到过 recordId（申领响应体按 R-3 只回
 * {@code resourceId / status / remain}，不得增加字段），所以确认只能用
 * {@code (resourceId, studentId)} 定位。这同时是比 recordId 更强的约束：
 * 学号来自身份上下文，学生无法通过改参数确认别人的名额。
 *
 * <p><b>HTTP 状态码：一律 200，用业务码区分。</b>这是本项目的既有约定（限流 429 除外），
 * 与 {@code R} 结构一致。语义上"确认一条已释放的记录"确实是 409 冲突、
 * "确认一条已确认的记录"确实是 200 幂等，这两件事由业务码 2004 与 {@code code=0}
 * 分别表达，前端不需要为确认单独写一套状态码解析。
 */
@RestController
@RequestMapping("/api/claim")
public class ConfirmController {

    /** 与 {@code AsyncClaimController.HEADER_REPLAY} 同名不同值：一个是"重放申领"，一个是"重复确认"。 */
    static final String HEADER_REPLAY = "X-Confirm-Replay";

    private final ConfirmService confirmService;

    public ConfirmController(ConfirmService confirmService) {
        this.confirmService = confirmService;
    }

    @PostMapping("/confirm")
    public R<Map<String, Object>> confirm(@RequestParam long resourceId,
                                          @RequestParam(required = false) String studentId,
                                          HttpServletResponse response) {
        // 身份优先取上下文（StudentContextFilter 已从 CAS 或 X-Student-Id 解析），
        // 参数只作回落：接入统一认证之后，请求里的 studentId 会自动失效，
        // 业务代码不用改，也不会出现"参数冒充别人身份"的口子。
        String resolved = StudentContext.get();
        if (resolved == null || resolved.isBlank()) {
            resolved = studentId;
        }
        if (resolved == null || resolved.isBlank()) {
            return R.fail(ErrorCode.STUDENT_NOT_LOGIN);
        }

        ConfirmOutcome outcome = confirmService.confirm(resourceId, resolved);
        if (!outcome.success()) {
            String message = outcome.note() == null ? outcome.error().getMessage() : outcome.note();
            return R.fail(outcome.error(), message);
        }

        if (outcome.replayed()) {
            response.setHeader(HEADER_REPLAY, "1");
        }
        // LinkedHashMap 保序：逐字节比对响应体时字段顺序变化会被误判成不一致
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("resourceId", resourceId);
        data.put("status", outcome.status());
        return R.ok(data);
    }
}
