package com.campusslot.consumer.record;

import com.campusslot.common.constant.MqTopic;
import com.campusslot.common.mq.ClaimMessage;
import com.campusslot.common.mq.ClaimMessageCodec;
import com.campusslot.common.mq.MqProperties;
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
 * {@code cg_record} 消费组：申领消息落库（D3）。
 *
 * <p><b>只订阅 tag = CLAIM。</b>这个消费者是 {@code quota_record} 的唯一写入者，
 * 凡是没打 CLAIM 标记的消息一律不该到达这里——{@code /api/dev/mq/send} 那种手工调试消息
 * 若被它收到，就是一条永远解析不了的消息。用 tag 过滤把它挡在投递之前，
 * 比在消费端"先解析、失败就丢"更早一层，也省一次无效投递。
 *
 * <p><b>毒消息必须 ACK 掉，不能返回 FAILURE。</b>解码失败的消息重试一万次也解不出来，
 * 返回 FAILURE 只会让它在队列里反复投递到上限、进死信、同时把这个队列后面的消息全部堵住。
 * 正确动作是丢弃 + ERROR + 计数，让人去看。
 * （判断依据：失败原因是<b>消息本身</b>还是<b>处理过程</b>。前者重试无意义，后者才该重试。）
 *
 * <p><b>什么时候返回 FAILURE</b>：只有 {@code TRANSIENT}——数据库暂时不可用。
 * 这时重试是唯一正确的选择，退避交给 Broker（消费端自己 sleep 会拖住整个队列）。
 */
@Component
@ConditionalOnProperty(prefix = "campusslot.mq", name = "enabled", havingValue = "true")
public class ClaimRecordConsumer {

    private static final Logger log = LoggerFactory.getLogger(ClaimRecordConsumer.class);

    private final MqProperties properties;
    private final ClaimPersistService persistService;
    private final int threads;
    private final Counter okCounter;
    private final Counter duplicateCounter;
    private final Counter driftCounter;
    private final Counter retryCounter;
    private final Counter poisonCounter;

    private PushConsumer consumer;

    public ClaimRecordConsumer(MqProperties properties,
                               ClaimPersistService persistService,
                               MeterRegistry meters,
                               @Value("${campusslot.consumer.threads:20}") int threads) {
        this.properties = properties;
        this.persistService = persistService;
        this.threads = threads;
        this.okCounter = meters.counter("campusslot.claim.persist", "result", "ok");
        this.duplicateCounter = meters.counter("campusslot.claim.persist", "result", "duplicate");
        this.driftCounter = meters.counter("campusslot.claim.persist", "result", "drift");
        this.retryCounter = meters.counter("campusslot.claim.persist", "result", "retry");
        this.poisonCounter = meters.counter("campusslot.claim.persist", "result", "poison");
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
                    .setConsumerGroup(MqTopic.CG_RECORD)
                    .setSubscriptionExpressions(Map.of(
                            MqTopic.QUOTA_CLAIMED, new FilterExpression(MqTopic.TAG_CLAIM)))
                    // 消费线程数受两条天花板约束：Topic 的队列数（16）和 DB 连接池（32）。
                    // 开 20 个线程已经超过队列数，实际并行度上限是 16；
                    // 再往上加线程只是让 16 个队列上的消息在本地抢连接，D10 会把这个拐点量出来。
                    .setConsumptionThreadCount(threads)
                    .setMaxCacheMessageCount(2000)
                    .setMessageListener(this::handle)
                    .build();

            log.info("cg_record 已启动 group={} topic={} tag={} threads={} endpoints={}",
                    MqTopic.CG_RECORD, MqTopic.QUOTA_CLAIMED, MqTopic.TAG_CLAIM, threads, properties.getEndpoints());
        } catch (ClientException e) {
            // 启动失败要吵，但不能把进程带崩：Broker 比应用晚起 30 秒是很常见的场景
            log.error("cg_record 启动失败 endpoints={}，检查 proxy 是否已启动、Topic 是否已创建",
                    properties.getEndpoints(), e);
        }
    }

    private ConsumeResult handle(MessageView message) {
        String body = StandardCharsets.UTF_8.decode(message.getBody()).toString();
        ClaimMessage claim;
        try {
            claim = ClaimMessageCodec.decode(body);
        } catch (RuntimeException e) {
            poisonCounter.increment();
            log.error("毒消息丢弃 msgId={} attempt={} body={}",
                    message.getMessageId(), message.getDeliveryAttempt(),
                    body.length() > 300 ? body.substring(0, 300) + "..." : body, e);
            return ConsumeResult.SUCCESS;
        }

        ClaimPersistService.Result result = persistService.persist(claim);
        switch (result.status()) {
            case PERSISTED -> {
                okCounter.increment();
                if (log.isDebugEnabled()) {
                    log.debug("落库成功 msgId={} resourceId={} studentId={}",
                            message.getMessageId(), claim.resourceId(), claim.studentId());
                }
                return ConsumeResult.SUCCESS;
            }
            case DUPLICATE -> {
                // 见 ClaimPersistService 类注释：这里**故意不回补** Redis。
                // 绝大多数情况是消息重投（上一次已经落库并扣过 remain），
                // 回补就等于凭空多一个名额，直接违反 R-1。
                duplicateCounter.increment();
                log.warn("重复消息未回补（正常=重投；异常=幂等键过期后重抢） studentId={} resourceId={} attempt={}",
                        claim.studentId(), claim.resourceId(), message.getDeliveryAttempt());
                return ConsumeResult.SUCCESS;
            }
            case REMAIN_EXHAUSTED -> {
                driftCounter.increment();
                log.error("P0 库存漂移：DB remain 已为 0，拒绝落库 resourceId={} studentId={}。"
                        + "重试无意义，已 ACK；pending 将持续为正，等 D9 对账修复",
                        claim.resourceId(), claim.studentId());
                return ConsumeResult.SUCCESS;
            }
            case COLLEGE_EXHAUSTED -> {
                driftCounter.increment();
                log.error("P0 学院配额漂移：DB 该学院 remain 已为 0 resourceId={} college={} studentId={}。已 ACK",
                        claim.resourceId(), claim.collegeCode(), claim.studentId());
                return ConsumeResult.SUCCESS;
            }
            case POISON -> {
                // 数据缺陷（字段超长 / CHECK / NOT NULL / 外键）—— 重试没有信息增益。
                // 必须 ACK：留在队列里它会以退避方式重投 16 次，每次都把这个队列后面的
                // 正常消息一起堵住，实测能让一轮 7 万条消息的 drain 永远等不到 pending=0。
                // 代价是 Redis 已经扣掉的名额没有落库记录（idemKeys > records），
                // 这件事由 D9 对账负责补回来，而不是靠无限重试。
                poisonCounter.increment();
                log.error("P0 消息数据缺陷已 ACK resourceId={} studentId={} attempt={} reason={}",
                        claim.resourceId(), claim.studentId(),
                        message.getDeliveryAttempt(), result.detail());
                return ConsumeResult.SUCCESS;
            }
            case TRANSIENT -> {
                retryCounter.increment();
                // 只有这一种情况该重试。消费端不 sleep：退避由 Broker 按重试次数控制，
                // 在消费线程里 sleep 会把同队列后面的消息一起拖住，堆积越滚越大。
                return ConsumeResult.FAILURE;
            }
            default -> {
                return ConsumeResult.FAILURE;
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("cg_record 关闭失败", e);
            }
        }
    }
}
