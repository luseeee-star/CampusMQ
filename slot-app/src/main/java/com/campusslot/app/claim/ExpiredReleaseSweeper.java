package com.campusslot.app.claim;

import com.campusslot.common.constant.AppTime;
import com.campusslot.common.entity.QuotaRecord;
import com.campusslot.common.mapper.QuotaRecordMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SYS-1 超时释放：把 {@code expire_at <= now} 的 {@code LOCKED} 记录扫出来释放。
 *
 * <p><b>扫表是当前唯一的释放路径</b>：{@code quota_release_delay} 定时消息只在
 * {@code MqTopic} / 生产者接口里有定义（5.x 任意时间消息），<b>尚未接入生产发送点，
 * 也没有业务消费端</b>（{@code sendDelay} 目前只被 {@code DevMqController} 的烟测调用）。
 * 设计上它负责"准时"、扫表负责"兜底"，两条路径都会复用
 * {@link ReleaseService#releaseExpired}，靠 {@code WHERE status = 'LOCKED'} 的条件更新
 * 保证同一条记录只被释放一次——但今天只有扫表在跑，别把注释当防线。
 *
 * <p><b>为什么是 {@code fixedDelay} 而不是 {@code fixedRate}</b>：先排除一个常见的错说法——
 * "{@code fixedRate} 超时会排队，最后停 10 秒再一口气跑 20 次"。这个机制在 Java 里不存在：
 * {@code ScheduledThreadPoolExecutor} 的周期任务只保留一个条目，执行超周期时后续执行
 * 只会"延后并紧接着开始"，既不并发，也不会堆成 N 份待执行。
 * 真正的差别是<b>占空比</b>：积压期 {@code fixedRate} 把两次执行之间的间隙压到 0，
 * 等于让释放吃满一整个线程——同池（{@code pool.size=2}）的 SYS-2 通知重试只剩一个线程可抢。
 * {@code fixedDelay} 每轮之间强留 500ms，给同池任务一个确定性窗口。
 * 代价是最坏检测延迟 = 单轮时长 + 500ms：稳态（空扫描几毫秒）约 0.5s，积压期约 1.3s。
 * 释放是兜底动作，这个代价可以接受；卡住调度线程不行。
 *
 * <p><b>NFR-5「释放误差 P99 &lt; 1s」只能是稳态口径</b>：稳态下延迟由 500ms 周期决定，
 * P99 在 0.5s 量级；<b>积压期结构上做不到 1s</b>——800ms 单轮预算 + 500ms 间隔
 * 本身就是 1.3s 的下限，与实现无关。{@code expire_at} 已于 2026-11-04 升到
 * {@code DATETIME(3)}，"到期"的判定不再有整秒粒度误差，边界实验可以测亚秒；
 * 但这只解决了<b>测量</b>，没有改变上面那个 1.3s 的结构下限。
 *
 * <p><b>追平预算（3 批 × 100 或 800ms，逐条判）</b>：活动结束时可能有上万人同时超时。
 * 不加预算的话，一次扫描会占着调度线程把整个积压跑完，期间 SYS-2 的重试全部延后，
 * 表现为"通知忽然晚了几十秒"——而根因是另一个任务抢了线程。
 * 预算在<b>每释放完一条之后</b>判一次，不是"每批之前判一次"：批前判的话，
 * elapsed=799ms 时仍会放整整一批（100 条 ≈ 1s）进去，单轮最坏变成 1.8s，预算等于没有。
 * 逐条判的上界是 800ms + 单条（约 13ms）；被中断那一批里没处理的记录仍是 LOCKED，
 * 下一轮会重新扫到，不需要回滚。
 *
 * <p><b>多实例部署</b>：每台机器都会跑这个任务，但 {@code releaseIfLocked} 的条件更新
 * 只会让一台返回 1。所以这里的正确性不依赖"只有一个实例在扫"，也不加分布式锁——
 * 加锁反而会在持锁机器挂掉时把释放停掉。
 */
@Component
@ConditionalOnProperty(name = "campusslot.release.sweep-enabled", havingValue = "true", matchIfMissing = true)
public class ExpiredReleaseSweeper {

    private static final Logger log = LoggerFactory.getLogger(ExpiredReleaseSweeper.class);

    /** 单批条数：100 条 ≈ 100 次「事务 + Lua」，正常在几十毫秒内跑完。 */
    private static final int BATCH = 100;

    /** 一轮最多追几批。追上（某批不满）就提前结束。 */
    private static final int MAX_BATCHES = 3;

    /** 一轮的硬时间预算（逐条判，不是批间判）。到点就把剩下的留给下一轮，别饿死同池的通知重试。 */
    private static final long BUDGET_MS = 800;

    private final QuotaRecordMapper recordMapper;
    private final ReleaseService releaseService;

    public ExpiredReleaseSweeper(QuotaRecordMapper recordMapper, ReleaseService releaseService) {
        this.recordMapper = recordMapper;
        this.releaseService = releaseService;
    }

    @Scheduled(fixedDelayString = "${campusslot.release.sweep-interval-ms:500}")
    public void sweep() {
        long start = System.currentTimeMillis();
        int batches = 0;
        int released = 0;
        int skipped = 0;
        int failed = 0;
        boolean budgetHit = false;

        while (batches < MAX_BATCHES) {
            // 开新批之前先判一次：上一批已经吃掉预算就不要再取了
            if (System.currentTimeMillis() - start >= BUDGET_MS) {
                budgetHit = true;
                break;
            }

            // 每批重新取时钟：一轮最多 800ms，用同一时刻会让"刚过期"的记录永远差一点。
            LocalDateTime now = AppTime.now();
            List<QuotaRecord> batch = recordMapper.selectExpiredLocked(now, BATCH);
            if (batch.isEmpty()) {
                break;
            }
            batches++;

            for (QuotaRecord record : batch) {
                // 逐条判预算：批前判一次的话，elapsed=799ms 时仍会放一整批进去，
                // 单轮最坏 1.8s。逐条判的上界是 800ms + 单条。没处理完的记录仍是 LOCKED，
                // 下一轮会重新扫到，不用回滚、也不会漏。
                if (System.currentTimeMillis() - start >= BUDGET_MS) {
                    budgetHit = true;
                    break;
                }
                try {
                    ReleaseOutcome outcome = releaseService.releaseExpired(record);
                    if (!outcome.success()) {
                        // 事务失败：记录仍是 LOCKED，下一轮还会被扫到。这里只记数不重试，
                        // 重试会把这轮预算耗在同一条坏记录上。
                        failed++;
                    } else if (outcome.replayed()) {
                        skipped++;
                    } else {
                        released++;
                    }
                } catch (RuntimeException e) {
                    // 单条抛异常不能中断整批：否则一条坏数据会让它后面所有记录永远释放不掉，
                    // 而且每一轮都从同一条开始，表现为"超时释放整体停了"。
                    failed++;
                    log.error("超时释放异常 resourceId={} studentId={} recordId={}",
                            record.getResourceId(), record.getStudentId(), record.getId(), e);
                }
            }

            // 预算在批内就打断了：剩下的留给下一轮，别再开新批
            if (budgetHit) {
                break;
            }

            // 不满一批说明已经追平，不用再查
            if (batch.size() < BATCH) {
                break;
            }
        }

        long cost = System.currentTimeMillis() - start;
        if (released > 0 || failed > 0) {
            log.info("超时释放 batches={} released={} skipped={} failed={} costMs={} budgetHit={}",
                    batches, released, skipped, failed, cost, budgetHit);
        } else {
            log.debug("超时释放 batches={} released=0 costMs={}", batches, cost);
        }
    }
}
