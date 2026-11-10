package com.campusslot.app.resource;

import com.campusslot.app.reconcile.ReconcileService;
import com.campusslot.common.constant.AppTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * S-3 到点自动关闭：{@code close_at} 一到，把 {@code PUBLISHED} 的活动改成 {@code CLOSED}，
 * 并补一份可归档的终版报表（R-7）。
 *
 * <p><b>为什么必须有一个"写方"（2026-11-10 补）</b>：在此之前 CLOSED 这个状态
 * <b>全项目没有任何一条写入路径</b>——{@code ReconcileJob} 只是<b>读</b>它
 * （{@code WHERE status = 'CLOSED' AND NOT EXISTS (SELECT 1 FROM quota_report ...)}），
 * 而 {@link ReconcileService#reconcile} 对非 PUBLISHED 的活动只走 {@code writeFinalReport}。
 * 也就是说：活动到点之后，申领会被 {@code ResourceMeta.isOpen()} 按 {@code close_at} 拒掉
 * （这一半是对的），但状态<b>永远停在 PUBLISHED</b>，{@code quota_report} <b>永远不会出现</b>。
 * 后果不是"少一个状态"，而是 R-7 的报表只能靠人手工触发——而那正是"归档证据"最不该依赖的东西。
 *
 * <p><b>申领链路不依赖这个任务</b>。{@code isOpen()} 判的是时间，不是状态，
 * 所以本任务晚几秒跑，只是"关活动"晚几秒生效，不会让关闭后的请求被放进来。
 * 这个事实决定了周期可以取 5 秒而不是 500ms：它影响的是<b>报表出现的时刻</b>，
 * 不是<b>能不能申领</b>。反过来，如果哪天有人把 {@code isOpen()} 改成只判状态，
 * 这个任务的周期就必须一起收紧——现在的写法有隐含耦合，写在这里免得下一个人改错。
 *
 * <p><b>多实例安全靠条件更新，不靠"只有一个实例会跑"</b>：
 * {@code WHERE status = 'PUBLISHED'} 让只有一台返回 1，其余返回 0 直接跳过。
 * 与 SYS-1 扫表、SYS-2 通知重试同一个模式——定时任务在 2→8 台水平扩时会重复执行，
 * 任何"只有一个实例"的前提都是错的。
 *
 * <p><b>UPDATE 里再判一次 {@code close_at <= now}</b>：SELECT 与 UPDATE 之间
 * 运营可能把截止时间往后改了（活动延期是这个系统里最正常的操作之一）。
 * 只在 SELECT 判的话，那次延期会被这次 UPDATE 直接覆盖成"已关闭"，
 * 而且学生手里的链接会突然变成"活动已结束"——比晚关 5 秒糟得多。
 *
 * <p><b>{@code now} 用 {@link AppTime#now()}，不用 {@code NOW()}</b>：
 * {@code close_at} 是应用侧按 Asia/Shanghai 写进去的 {@code LocalDateTime}，
 * 而 {@code NOW()} 取服务器时区。CI 容器默认 UTC 时，这条 SQL 会整体偏移 8 小时，
 * 表现是"活动提前 8 小时关闭"或"永远不关闭"——两种都很难从日志上看出来。
 */
@Component
@ConditionalOnProperty(name = "campusslot.resource.close-scan-enabled",
        havingValue = "true", matchIfMissing = true)
public class ResourceCloseJob {

    private static final Logger log = LoggerFactory.getLogger(ResourceCloseJob.class);

    /** 单轮处理上限。真实场景一次只有几个活动同时到点，50 只是防"批量灌测试数据"时一轮拉太久。 */
    private static final int BATCH = 50;

    private final JdbcTemplate jdbc;
    private final ReconcileService reconcileService;

    public ResourceCloseJob(JdbcTemplate jdbc,
                            ReconcileService reconcileService,
                            @Value("${campusslot.resource.close-scan-interval-ms:5000}") long intervalMs) {
        this.jdbc = jdbc;
        this.reconcileService = reconcileService;
        // intervalMs 由 @Scheduled 直接读取，这里接一下只是为了让启动日志能打出来
        log.info("到点自动关闭已启用：扫描周期 {} ms（关闭生效只影响报表，申领由 close_at 判定）", intervalMs);
    }

    @Scheduled(fixedDelayString = "${campusslot.resource.close-scan-interval-ms:5000}")
    public void run() {
        int closed = 0;
        int reports = 0;
        int errors = 0;

        LocalDateTime now = AppTime.now();

        List<Long> due;
        try {
            // 走 idx_dept_status 的 status 前缀（dept 维度这里不需要，是全量扫描关闭）。
            // close_at 上没有索引：这张表是一行一个活动，量级是几十到几百，
            // 为"每 5 秒扫一次的几十行"再加一个索引，维护成本比它省下的读更大。
            due = jdbc.queryForList("""
                    SELECT id FROM quota_resource
                     WHERE status = 'PUBLISHED' AND close_at <= ?
                     ORDER BY close_at
                     LIMIT ?
                    """, Long.class, now, BATCH);
        } catch (RuntimeException e) {
            log.error("到点自动关闭：查询待关闭活动失败，本轮跳过", e);
            return;
        }

        for (Long resourceId : due) {
            try {
                int rows = jdbc.update("""
                        UPDATE quota_resource
                           SET status = 'CLOSED', version = version + 1, updated_at = ?
                         WHERE id = ? AND status = 'PUBLISHED' AND close_at <= ?
                        """, now, resourceId, now);
                if (rows == 0) {
                    // 另一台实例赢了这次条件更新，或者活动在这几毫秒里被延期了。
                    // 两种情况都不该在这里补报表：前者对方会写，后者本来就不该关。
                    continue;
                }
                closed++;
                log.info("活动到点自动关闭 resourceId={} closeAt<={} —— 申领在此之前已按时间判据拒绝", resourceId, now);
            } catch (RuntimeException e) {
                errors++;
                // 单个活动失败不能中断整轮，也不能中断后面的报表兜底
                log.error("到点自动关闭：单个活动处理失败 resourceId={}（本轮继续）", resourceId, e);
            }
        }

        // 报表兜底：CLOSED 但没有 quota_report 的活动。
        // 需要它是因为"改状态"和"写报表"不是同一个事务（报表要统计几万条记录，
        // 不该把 quota_resource 的行锁在上面）。中间被杀进程、或状态是别人改的，
        // 就会留下一个"已关闭但没报表"的活动。writeFinalReport 自己幂等，重复调不会多写。
        try {
            List<Long> needReport = jdbc.queryForList("""
                    SELECT r.id FROM quota_resource r
                     WHERE r.status = 'CLOSED'
                       AND NOT EXISTS (SELECT 1 FROM quota_report p WHERE p.resource_id = r.id)
                     ORDER BY r.close_at DESC
                     LIMIT ?
                    """, Long.class, BATCH);
            for (Long resourceId : needReport) {
                try {
                    if (reconcileService.writeFinalReport(resourceId).reportWritten()) {
                        reports++;
                    }
                } catch (RuntimeException e) {
                    errors++;
                    log.error("到点自动关闭：补终版报表失败 resourceId={}（下轮重试）", resourceId, e);
                }
            }
        } catch (RuntimeException e) {
            errors++;
            log.error("到点自动关闭：查询缺报表的已关闭活动失败，本轮跳过", e);
        }

        // 平稳时不打日志：每 5 秒一条"一切正常"会把真正有用的那几条埋掉
        if (closed > 0 || reports > 0 || errors > 0) {
            log.info("到点自动关闭一轮：关闭 {} 个，补报表 {} 份，失败 {} 个", closed, reports, errors);
        } else {
            log.debug("到点自动关闭一轮：无需处理");
        }
    }
}
