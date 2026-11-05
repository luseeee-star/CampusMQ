package com.campusslot.app.reconcile;

import com.campusslot.app.reconcile.dto.ReconcileOutcome;
import com.campusslot.common.api.R;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 对账巡检的运维入口（SYS-3）。
 *
 * <p><b>只暴露"跑一个活动"，没有"跑全部"</b>。原因不是懒得写，是这个接口没有鉴权：
 * 一个能触发全表扫描的免鉴权 POST，等于给了一套自带放大器的压测工具——
 * 调用方循环打它就能让数据库的连接池一直满着，而且表面上看只是"访问了一个管理接口"。
 * 定时任务本来就负责全量，手动入口的用途只有一个：验收时不想等满一个周期。
 *
 * <p><b>这个接口会写表</b>（{@code reconcile_diff} / {@code quota_report}），不是只读的。
 * 所以用 POST，不用 GET——GET 会被浏览器预取、被监控系统定期探测，那会莫名其妙多出一堆差异记录。
 * 巡检本身只"发现和留证"，不修数据：{@code fixed_at} 一律留空，由人确认后回填。
 */
@RestController
@RequestMapping("/api/admin/reconcile")
public class ReconcileController {

    private final ReconcileService service;

    public ReconcileController(ReconcileService service) {
        this.service = service;
    }

    /**
     * 立刻对某个活动做一次比对。
     *
     * <p>返回的是这一次的结论，不是"历史所有差异"——历史差异查 {@code reconcile_diff}，
     * 或看 `deploy/sql/report.sql` 第 4 段（未修复的差异）。
     */
    @PostMapping("/run")
    public R<ReconcileOutcome> run(@RequestParam long resourceId) {
        return R.ok(service.reconcile(resourceId));
    }
}
