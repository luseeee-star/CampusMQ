package com.campusslot.app.claim;

import com.campusslot.common.api.R;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 申领入口 · MQ 异步落库版（对照组③，D3 起的主链路）。
 *
 * <p>路径与 D1/D2 分开是刻意的：{@code /api/baseline/claim}（裸 DB）、
 * {@code /api/claim}（Redis 同步落库）、{@code /api/claim/mq}（Redis + MQ 异步落库）
 * 三条路径一一对应三组对照实验。压测时只改压测器的 path 参数，线程模型完全不变；
 * 若共用一个路径靠开关切换，某次忘了切就会把两轮数据混成一组，而数据看起来完全正常。
 *
 * <p><b>响应体的字段是"合同"，不是"调试信息"。</b>
 * R-3 要求重复请求返回首次结果，所以这 3 个字段必须能被重放逐字节复现：
 * <ul>
 *   <li>{@code resourceId} 请求参数，天然一致</li>
 *   <li>{@code status} 恒为 LOCKED，首次扣减成功就一定是这个状态</li>
 *   <li>{@code remain} 幂等键里存的「首次扣减后的余量」，不是"现在还剩多少"</li>
 * </ul>
 * 想加「这是第几次提交」「当前真实余量」这类字段之前，先想清楚它们会不会让重放
 * 与首次产生差异。会，就别加进 body。
 *
 * <p>需要区分首次与重放时看响应头 {@code X-Claim-Replay}：
 * body 保持一致是需求，可观测性是工程需要，两者用不同通道。
 */
@RestController
@RequestMapping("/api/claim/mq")
public class AsyncClaimController {

    static final String HEADER_REPLAY = "X-Claim-Replay";

    private final AsyncClaimService claimService;

    public AsyncClaimController(AsyncClaimService claimService) {
        this.claimService = claimService;
    }

    @PostMapping
    public R<Map<String, Object>> claim(@RequestParam long resourceId,
                                        @RequestParam String studentId,
                                        HttpServletResponse response) {
        ClaimOutcome outcome = claimService.claim(resourceId, studentId);
        if (!outcome.success()) {
            String message = outcome.note() == null ? outcome.error().getMessage() : outcome.note();
            return R.fail(outcome.error(), message);
        }
        if (outcome.replayed()) {
            response.setHeader(HEADER_REPLAY, "1");
        }
        // 字段顺序固定：用 LinkedHashMap 而不是 Map.of，后者顺序不保证，
        // 逐字节比对响应体时会因为字段顺序不同而误判成"不一致"。
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("resourceId", resourceId);
        data.put("status", outcome.status());
        data.put("remain", outcome.remain());
        return R.ok(data);
    }
}
