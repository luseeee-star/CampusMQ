package com.campusslot.app.claim;

import com.campusslot.common.api.R;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 申领入口（S-1 抢名额）。
 *
 * <p>路径与参数刻意和 D1 的 {@code /api/baseline/claim?activityId=} 区分开：
 * 两个对照组用两套接口，压测时只改压测器的 path 参数，线程模型完全不变。
 * 如果让它们共用一个路径靠开关切换，很容易在某次压测里忘了切，把两轮数据混成一组。
 *
 * <p>返回码约定（压测器按 code 分类计数）：
 * <ul>
 *   <li>{@code 0} 抢到</li>
 *   <li>{@code 2001} 名额已满</li>
 *   <li>{@code 2006} 重复申领（D4 会改成原样返回首次结果）</li>
 *   <li>{@code 9xxx} 系统级，压测时必须为 0，不为 0 说明这一轮数据作废</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/claim")
public class ClaimController {

    private final ClaimService claimService;

    public ClaimController(ClaimService claimService) {
        this.claimService = claimService;
    }

    @PostMapping
    public R<Map<String, Object>> claim(@RequestParam long resourceId,
                                        @RequestParam String studentId) {
        ClaimOutcome outcome = claimService.claim(resourceId, studentId);
        if (!outcome.success()) {
            String message = outcome.note() == null ? outcome.error().getMessage() : outcome.note();
            return R.fail(outcome.error(), message);
        }
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("resourceId", resourceId);
        data.put("status", outcome.status());
        return R.ok(data);
    }
}
