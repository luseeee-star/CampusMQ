package com.campusslot.app.notify;

import com.campusslot.common.constant.AppTime;
import com.campusslot.common.entity.Notification;
import com.campusslot.common.mapper.NotificationMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SYS-2 的重试器：把 {@code notification} 里到期的行发出去，失败按指数退避排下一次。
 *
 * <p><b>取数的 SQL 直接复用 {@link NotificationMapper#selectRetryable}</b>，不另写一份。
 * 那份谓词有三个条件（{@code status IN ('PENDING','FAILED')}、{@code retry_count < maxRetry}、
 * {@code next_retry_at IS NULL OR <= now}），少任何一个都会出静默故障：
 * 只写 PENDING 会让"失败一次就再也不重试"，漏掉 {@code IS NULL} 会让
 * "新写入的行首次发送永远不触发"（insertIgnore 不填 next_retry_at，新行就是 NULL）。
 *
 * <p><b>DEAD 在最后一次失败的同一轮里置</b>，不等下一轮扫描再判：
 * {@code retry_count < maxRetry} 意味着到达上限的行再也捞不出来，
 * 若不在这一轮置 DEAD，它就永远停在 FAILED——既不重试也不 DEAD，报表里是一条幽灵行。
 *
 * <p><b>预算逐条判</b>（和扫表同一个教训）：批前判的话，剩余预算 1ms 时仍会放整批 50 条进去，
 * 一轮变成 50 × 单条耗时。这里单条只花一次 DB UPDATE，但多实例 + 5 万条积压时同样会把
 * 调度池的一根线程占满，而同池还排着每 500ms 一次的释放扫描。
 */
@Component
@ConditionalOnProperty(name = "campusslot.notify.retry-enabled", havingValue = "true", matchIfMissing = true)
public class NotificationRetryJob {

    private static final Logger log = LoggerFactory.getLogger(NotificationRetryJob.class);

    private final NotificationMapper mapper;
    private final MockNotifySender sender;
    private final int maxRetry;
    private final long backoffMs;
    private final int batch;
    private final long budgetMs;

    public NotificationRetryJob(NotificationMapper mapper,
                                MockNotifySender sender,
                                @Value("${campusslot.notify.max-retry:7}") int maxRetry,
                                @Value("${campusslot.notify.retry-backoff-ms:1000}") long backoffMs,
                                @Value("${campusslot.notify.retry-batch:50}") int batch,
                                @Value("${campusslot.notify.retry-budget-ms:600}") long budgetMs) {
        this.mapper = mapper;
        this.sender = sender;
        this.maxRetry = maxRetry;
        this.backoffMs = backoffMs;
        this.batch = batch;
        this.budgetMs = budgetMs;
    }

    @Scheduled(fixedDelayString = "${campusslot.notify.retry-interval-ms:1000}")
    public void retry() {
        long start = System.currentTimeMillis();
        int sent = 0;
        int retried = 0;
        int dead = 0;
        boolean budgetHit = false;

        while (true) {
            if (System.currentTimeMillis() - start >= budgetMs) {
                budgetHit = true;
                break;
            }

            // 每批重新取时钟：退避时刻按"现在"算，一轮跨了秒边界也不会排到过去
            LocalDateTime now = AppTime.now();
            List<Notification> rows = mapper.selectRetryable(now, maxRetry, batch);
            if (rows.isEmpty()) {
                break;
            }

            for (Notification n : rows) {
                if (System.currentTimeMillis() - start >= budgetMs) {
                    budgetHit = true;
                    break;
                }
                int attempt = (n.getRetryCount() == null ? 0 : n.getRetryCount()) + 1;
                boolean ok;
                try {
                    ok = sender.send(n, attempt);
                } catch (RuntimeException e) {
                    // 通道抛异常与"返回失败"等价，不能让它中断整批：
                    // 一条坏数据会让它后面的所有通知永远发不出去。
                    ok = false;
                    log.warn("通知通道异常 id={} bizId={} attempt={}", n.getId(), n.getBizId(), attempt, e);
                }

                if (ok) {
                    mapper.markSent(n.getId(), attempt, AppTime.now());
                    sent++;
                    continue;
                }

                if (attempt >= maxRetry) {
                    // 同轮置 DEAD，理由见类注释
                    mapper.markDead(n.getId(), attempt, "mock channel failed after " + attempt + " attempts");
                    dead++;
                    log.warn("通知进入 DEAD（停止自动重试，等待人工重发） id={} bizId={} attempts={}",
                            n.getId(), n.getBizId(), attempt);
                } else {
                    long delay = backoffMs << (attempt - 1);
                    long nextAtMs = AppTime.nowMs() + delay;
                    mapper.markRetry(n.getId(), attempt, AppTime.fromEpochMs(nextAtMs),
                            "mock channel failed, attempt " + attempt);
                    // 把退避值本身打出来，而不是事后从 sent_at - created_at 反推。
                    // 反推出来的是「调度延迟」不是「退避间隔」：fixedDelay=1000 意味着
                    // 下一轮要等下一个 tick，中间固定夹 0~1s 抖动，这个抖动与 DATETIME 列
                    // 是 0 还是 3 位精度无关。1/2/4/8/16/32 只有从这里读才是干净的。
                    log.info("通知退避 id={} bizId={} attempt={} delayMs={} nextAt={}",
                            n.getId(), n.getBizId(), attempt, delay, AppTime.fromEpochMs(nextAtMs));
                    retried++;
                }
            }

            if (budgetHit) {
                break;
            }
            if (rows.size() < batch) {
                break;
            }
        }

        long cost = System.currentTimeMillis() - start;
        if (sent + retried + dead > 0) {
            log.info("通知重试 sent={} retried={} dead={} costMs={} budgetHit={} failAttempts={}",
                    sent, retried, dead, cost, budgetHit, sender.failAttempts());
        } else {
            log.debug("通知重试 sent=0 costMs={}", cost);
        }
    }
}
