package com.campusslot.app.reconcile;

import com.campusslot.app.reconcile.dto.ReconcileOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 每 5 分钟的对账巡检（SYS-3）。
 *
 * <p><b>默认关闭</b>（{@code matchIfMissing = false}）。理由不是"它不重要"，而是它的运行前提很窄：
 * 压测期间它每 5 分钟扫一遍全部活动、写 {@code reconcile_diff} 和 {@code quota_report}，
 * 会抢数据库连接，还会把 B 块注入的故障（删记录）提前报出来——那时候"巡检先发现"和
 * "注入生效"两条信号会混在一起，分不清是谁先动的。所以它的生命周期是：
 * 验收时开、验完关、B 块结束再开。
 *
 * <p>默认关还有一个更实际的考虑：忘记关的代价不对称。忘了开只是"暂时没巡检"，
 * 忘了关会污染一整天压测的数据。默认值应该偏向更难犯错的那一侧。
 *
 * <p><b>遍历方向是"从活动表出发"，不是"从 Redis key 出发"</b>。反过来 {@code SCAN quota:stock:*}
 * 再回查 DB，会多报一类和账无关的差异：2026-11-05 清理环境时发现一个孤儿 key
 * {@code quota:stock:9001}（值 99,851,406），来源是 D7 天花板探针
 * {@code benchmark/ceiling.ps1}——它全程用 {@code resourceId=9001} 打纯 Lua 端点，
 * 不建活动、不落库，但会一次次 {@code DECR} 同一个 key。活动表里没有 9001，
 * 反向扫就会把它报成"Redis 有、DB 无"，和真正要看的 ±3 混在一起。
 * 孤儿 key 归清理脚本管，不进巡检告警。
 *
 * <p><b>单轮有上限，且用游标避免饥饿</b>：{@code batch-size} 限制一轮处理多少个活动。
 * 如果每轮都从 {@code id} 最小的开始取 {@code LIMIT}，活动数一旦超过上限，
 * 尾部的活动永远轮不到——而且它们恰恰是最新发布、最需要盯的那批。
 * 所以用一个只在内存里的游标（上一轮扫到的最大 id），扫到底再从头开始绕。
 * 多实例时每个实例各持一个游标，扫的是不同段，合起来仍然全覆盖；停机则从头开始。
 */
@Component
@ConditionalOnProperty(name = "campusslot.reconcile.enabled", havingValue = "true", matchIfMissing = false)
public class ReconcileJob {

    private static final Logger log = LoggerFactory.getLogger(ReconcileJob.class);

    private final JdbcTemplate jdbc;
    private final ReconcileService service;
    private final int batchSize;

    /** 上一轮扫到的最大 id。只在内存里：重启从头开始，代价是"最坏多扫一轮"。 */
    private long cursor = 0L;

    public ReconcileJob(JdbcTemplate jdbc,
                        ReconcileService service,
                        @Value("${campusslot.reconcile.batch-size:200}") int batchSize) {
        this.jdbc = jdbc;
        this.service = service;
        this.batchSize = Math.max(1, batchSize);
    }

    @Scheduled(fixedDelayString = "${campusslot.reconcile.interval-ms:300000}")
    public synchronized void run() {
        List<Target> targets = poll();
        if (targets.isEmpty()) {
            return;
        }

        int scanned = 0;
        int diffs = 0;
        int keyMissing = 0;
        int finals = 0;
        int errors = 0;

        for (Target t : targets) {
            try {
                if ("CLOSED".equals(t.status())) {
                    ReconcileOutcome o = service.writeFinalReport(t.id());
                    if (o.reportWritten()) {
                        finals++;
                    }
                } else {
                    ReconcileOutcome o = service.reconcile(t.id());
                    scanned++;
                    if (ReconcileService.ACTION_DIFF.equals(o.action())) {
                        diffs++;
                    }
                    if (o.stockKeyMissing()) {
                        keyMissing++;
                    }
                }
            } catch (Exception e) {
                // 单个活动失败不能中断整轮：一个活动数据异常不该让其他活动停止被巡检
                errors++;
                log.error("对账：单个活动处理失败 resourceId={}（本轮继续）", t.id(), e);
            } finally {
                cursor = t.id();
            }
        }

        // 全平的时候不打日志：5 分钟一条"一切正常"会把真正有用的那几条埋掉
        if (diffs + keyMissing + finals + errors > 0) {
            log.info("对账一轮：扫描 {} 个活动，差异 {} 条，库存 key 缺失 {} 个，补终版报表 {} 份，失败 {} 个",
                    scanned, diffs, keyMissing, finals, errors);
        } else {
            log.debug("对账一轮：扫描 {} 个活动，全部平衡", scanned);
        }
    }

    /**
     * 取本轮要处理的活动。
     *
     * <p>两类目标：已发布的（比对）、已关闭但还没有报表的（补一份终版）。
     * {@code DRAFT} 不取——它还没有 {@code quota:stock} key，比出来必然是"key 缺失"。
     * {@code OFFLINE} 不取——下架的活动不再对外，历史账由已有的报表承担。
     */
    private List<Target> poll() {
        List<Target> list = queryAfter(cursor);
        if (list.isEmpty() && cursor > 0) {
            // 绕回开头。不清空已有的 diff 记录，只是重新从最小 id 开始扫。
            cursor = 0L;
            list = queryAfter(0L);
        }
        return list;
    }

    private List<Target> queryAfter(long afterId) {
        return jdbc.query("""
                        SELECT r.id, r.status
                          FROM quota_resource r
                         WHERE r.id > ?
                           AND (r.status = 'PUBLISHED'
                                OR (r.status = 'CLOSED'
                                    AND NOT EXISTS (SELECT 1 FROM quota_report p WHERE p.resource_id = r.id)))
                         ORDER BY r.id
                         LIMIT ?
                        """,
                (rs, i) -> new Target(rs.getLong("id"), rs.getString("status")),
                afterId, batchSize);
    }

    private record Target(long id, String status) {
    }
}
