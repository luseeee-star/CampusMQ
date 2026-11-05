package com.campusslot.consumer.notify;

import com.campusslot.common.constant.MqTopic;
import com.campusslot.common.mq.ClaimMessage;
import com.campusslot.common.mq.ClaimMessageCodec;
import com.campusslot.common.mq.MqProperties;
import com.campusslot.common.notify.NotificationService;
import com.campusslot.common.notify.NotifyEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * {@code cg_notify} 消费组：锁定通知（SYS-2）。
 *
 * <p><b>为什么锁定通知写在这里，而不是写在申领主链路里</b>：
 * 申领路径是 {@code Redis 扣减 → 发消息 → 立即返回}，本地没有事务可以挂。
 * 在那里写通知要么多一次同步 DB 往返（打回 11k/s 的主链路），
 * 要么写成"发了消息但没写通知"的窗口。而这条消息本来就要被 {@code cg_record} 消费一次，
 * 同一条消息再被本组收一份的边际成本接近于零——这就是 RocketMQ「组间广播」的正当用法。
 *
 * <p><b>它与主链路的关系是"可停"</b>：停掉本消费组，申领照样成功（D10 的
 * 「停通知消费者不影响主链路」就是靠这个结构成立的）。所以这里任何失败都只能影响通知，
 * 不能反压到写库那条链路——两者是不同的消费组，位点互不影响。
 *
 * <p><b>幂等靠唯一键，不靠位点。</b>消息重投时 {@code INSERT IGNORE} 命中
 * {@code uk(biz_id, channel)}，第二次是 no-op。所以这里不需要"先查再写"，
 * 也不需要消费位点去重——那两样在重投面前都不可靠。
 *
 * <p><b>为什么还要判"消息年龄"</b>（2026-11-04 实测踩出来的）：新建消费组没有位点，
 * 第一次订阅会把 Topic 里<b>所有保留的历史消息</b>重放一遍。这台机器上实测一启动就灌了
 * 3.5 万条锁定通知，全是几天前压测的学生——他们的记录早被 reset 清掉，通知没有任何意义。
 * 对 {@code cg_notify} 这种"新建组"更是常态。位点重置本可以挡住，但 gRPC 消费端走 POP 模式，
 * {@code mqadmin resetOffsetByTime} 写的是经典位点表，实测不生效（topic 最大位点 205,34x，
 * 重置后重启仍从 0 开始）。
 *
 * <p>所以判据必须落在消息自己身上：{@code bornTimestamp} 超过 {@code max-age-ms} 且
 * <b>从未重投过</b>（{@code deliveryAttempt <= 1}）的一律跳过。两个条件缺一不可——
 * 只看 age 会把"失败很久后终于该补写"的重投消息一起吞掉（见 {@code handle} 里的注释）。
 * 这同时也是一条业务规则——"你抢到了名额"这种事晚到十分钟已经没有价值（确认窗口才 30 分钟），
 * 晚到三天的通知只会让人以为系统坏了。跳过是 ACK，不是重试：重试只会让积压更长。
 */
@Component
@ConditionalOnProperty(prefix = "campusslot.mq", name = "enabled", havingValue = "true")
public class LockNotifyConsumer {

    private static final Logger log = LoggerFactory.getLogger(LockNotifyConsumer.class);

    private final MqProperties properties;
    private final NotificationService notificationService;
    private final long maxAgeMs;
    private final String group;
    private final Counter enqueuedCounter;
    private final Counter poisonCounter;
    private final Counter retryCounter;
    /** 只用于 stale 分桶计数：桶值在运行时算，预先建 4 个 counter 不如查一次 map。 */
    private final MeterRegistry meters;

    private PushConsumer consumer;

    public LockNotifyConsumer(MqProperties properties,
                              NotificationService notificationService,
                              MeterRegistry meters,
                              @Value("${campusslot.notify.max-age-ms:600000}") long maxAgeMs,
                              @Value("${campusslot.notify.lock-group:" + MqTopic.CG_NOTIFY + "}") String group) {
        this.properties = properties;
        this.notificationService = notificationService;
        this.meters = meters;
        this.maxAgeMs = maxAgeMs;
        this.group = group;
        this.enqueuedCounter = meters.counter("campusslot.notify.enqueue", "result", "ok");
        this.poisonCounter = meters.counter("campusslot.notify.enqueue", "result", "poison");
        this.retryCounter = meters.counter("campusslot.notify.enqueue", "result", "retry");
        start();
    }

    private void start() {
        try {
            ClientServiceProvider provider = ClientServiceProvider.loadService();
            ClientConfiguration configuration = ClientConfiguration.newBuilder()
                    .setEndpoints(properties.getEndpoints())
                    .build();

            this.consumer = provider.newPushConsumerBuilder()
                    .setClientConfiguration(configuration)
                    .setConsumerGroup(group)
                    .setSubscriptionExpressions(Map.of(
                            MqTopic.QUOTA_CLAIMED, new FilterExpression(MqTopic.TAG_CLAIM)))
                    // 只写一张表、一条 INSERT，5 个线程足够：真正的积压会被 D10 的故障矩阵
                    // （停本消费组）暴露，而不是靠这里堆线程。
                    .setConsumptionThreadCount(5)
                    .setMaxCacheMessageCount(1000)
                    .setMessageListener(this::handle)
                    .build();

            log.info("cg_notify 已启动 group={} topic={} tag={} maxAgeMs={} endpoints={}",
                    group, MqTopic.QUOTA_CLAIMED, MqTopic.TAG_CLAIM, maxAgeMs, properties.getEndpoints());
        } catch (ClientException e) {
            // 与 cg_record 同一处置：Broker 比应用晚起很常见，吵一声但不要把进程带崩
            log.error("cg_notify 启动失败 endpoints={}，检查 proxy 是否已启动、Topic 是否已创建",
                    properties.getEndpoints(), e);
        }
    }

    private ConsumeResult handle(MessageView message) {
        // 时效判据放在解码之前：历史重放的量可能有几十万条，连 JSON 都不该解析
        long age = System.currentTimeMillis() - message.getBornTimestamp();
        // 但只能跳过"首次投递"的旧消息。bornTimestamp 是生产时刻，重投不会刷新它——
        // Broker 侧给的是重投次数上限 retryMaxTimes=16，<b>重投间隔本身是一条逐级放大的阶梯</b>
        // （10s 起、逐级拉长，累计可到小时级）。所以"一条 09:00 生产、09:11 还在重投"完全可能，
        // 而且它正是"DB 刚恢复、该补写"的那一条，按旧逻辑会被当成历史消息 ACK 掉，
        // 通知永久静默丢失。判据必须落在投递次数上：只有从未重投过的旧消息才是"历史回放"。
        //
        // 注意 consumeTimeoutMinute 不是重投间隔：它是"消息被拉走后多久没 ACK 算超时"，
        // 两个数并列写会让人以为重投窗口只有 15 分钟，从而低估这个问题。
        int attempt = message.getDeliveryAttempt();
        if (age > maxAgeMs) {
            if (attempt <= 1) {
                countStale(attempt, true);
                // 每条都打日志会淹掉文件（重放十万条就十万行），只在超龄时记 debug
                if (log.isDebugEnabled()) {
                    log.debug("跳过超龄锁定通知 msgId={} ageMs={} attempt={}",
                            message.getMessageId(), age, attempt);
                }
                return ConsumeResult.SUCCESS;
            }
            // 超龄但确实重投过 → 放行入账。这里必须单独计数，否则"判据到底区分了什么"
            // 只存在于推理里：stale 桶永远是 attempt<=1，看不出另一侧有没有被放过。
            countStale(attempt, false);
        }

        String body = StandardCharsets.UTF_8.decode(message.getBody()).toString();
        ClaimMessage claim;
        try {
            claim = ClaimMessageCodec.decode(body);
        } catch (RuntimeException e) {
            // 毒消息重试一万次也解不出来，只会堵住队列后面的人
            poisonCounter.increment();
            log.error("通知侧毒消息丢弃 msgId={} attempt={}", message.getMessageId(), e);
            return ConsumeResult.SUCCESS;
        }

        try {
            notificationService.enqueue(NotifyEvent.LOCK, claim.resourceId(), claim.studentId());
            enqueuedCounter.increment();
            return ConsumeResult.SUCCESS;
        } catch (RuntimeException e) {
            // 只有"处理过程"失败才该重试（DB 暂时不可用）；数据缺陷会让 INSERT IGNORE 静默吞掉，
            // 走不到这里。
            retryCounter.increment();
            log.warn("通知入账失败，交给 Broker 重投 resourceId={} studentId={} attempt={}",
                    claim.resourceId(), claim.studentId(), message.getDeliveryAttempt(), e);
            return ConsumeResult.FAILURE;
        }
    }

    /**
     * 超龄消息按 {@code deliveryAttempt} 分桶计数。
     *
     * <p>为什么不能只记一个 {@code result=stale} 的总量：{@code attempt <= 1} 这个判据的
     * 唯一风险是<b>恒真</b>——如果 POP 客户端对所有投递都填 0 或 1，修复等价于没改，
     * 而从一个只有总量的计数器上完全看不出来。分桶之后四种组合各有含义：
     * <ul>
     *   <li>只有 {@code stale/attempt=1} 涨、{@code stale_kept} 全 0 → 没有重投消息来过，
     *       判据尚未被检验（正常状态，D10 停 DB 才会出现下一种）</li>
     *   <li>{@code stale/attempt=0} 涨 → {@code deliveryAttempt} 根本没填，
     *       "从未重投"与"重投了但客户端不报"分不开，这是最坏情况</li>
     *   <li>{@code stale_kept/attempt=2-5} 或 {@code 6+} 涨 → 判据真的在区分，
     *       重投消息被放行入账了，这才是修复生效的直接证据</li>
     * </ul>
     *
     * <p>{@code stale_kept} 是"超龄但放行"，不是失败：它的作用是把"判据放过了什么"
     * 也变成可观测的，否则只能靠读代码推理另一侧。
     */
    private void countStale(int attempt, boolean skipped) {
        String bucket = attempt <= 0 ? "0" : attempt == 1 ? "1" : attempt <= 5 ? "2-5" : "6+";
        meters.counter("campusslot.notify.enqueue",
                "result", skipped ? "stale" : "stale_kept",
                "attempt", bucket).increment();
    }

    @PreDestroy
    public void shutdown() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("cg_notify 关闭失败", e);
            }
        }
    }
}
