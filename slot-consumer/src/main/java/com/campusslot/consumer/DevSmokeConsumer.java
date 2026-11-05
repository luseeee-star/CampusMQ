package com.campusslot.consumer;

import com.campusslot.common.constant.MqTopic;
import com.campusslot.common.mq.MqProperties;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * dev 冒烟消费者：只收 {@code smoke} / {@code delay} 两个 tag，打印「出生时刻 → 消费时刻」的差。
 *
 * <p>它承载 D1 的三个验证点之一——<b>定时消息的实际精度</b>。文档说「秒级精度」是组件的能力，
 * 不是你的链路上的精度；精度会被消费线程池、堆积、GC 影响，只能自己测。
 *
 * <p><b>为什么单独一个消费组</b>：它收的是手工调试消息，绝不能和 {@code cg_record} 混在一起。
 * D3 之前两者共用 {@code cg_record}，那时候消费者只打日志所以无害；
 * 一旦 {@code cg_record} 开始写 {@code quota_record}，每调一次 {@code /api/dev/mq/send}
 * 就往库里塞一条解析不了的消息。
 * <b>消费组的边界应该由"写什么"决定，不是由"读哪个 Topic"决定。</b>
 */
@Component
@ConditionalOnProperty(prefix = "campusslot.mq", name = "enabled", havingValue = "true")
public class DevSmokeConsumer {

    private static final Logger log = LoggerFactory.getLogger(DevSmokeConsumer.class);

    /** 两个 tag 用 || 表达「或」，这是 RocketMQ tag 过滤的原生语法。 */
    private static final String SMOKE_TAGS = "smoke||delay";

    private final MqProperties properties;
    private PushConsumer consumer;

    public DevSmokeConsumer(MqProperties properties) {
        this.properties = properties;
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
                    .setConsumerGroup(MqTopic.CG_SMOKE)
                    .setSubscriptionExpressions(Map.of(
                            MqTopic.QUOTA_CLAIMED, new FilterExpression(SMOKE_TAGS),
                            MqTopic.QUOTA_RELEASE_DELAY, FilterExpression.SUB_ALL))
                    .setMessageListener(this::handle)
                    .build();

            log.info("dev 冒烟消费者已启动 group={} tags={}", MqTopic.CG_SMOKE, SMOKE_TAGS);
        } catch (ClientException e) {
            log.error("dev 冒烟消费者启动失败 endpoints={}，检查 proxy 是否已启动、Topic 是否已创建",
                    properties.getEndpoints(), e);
        }
    }

    private ConsumeResult handle(MessageView message) {
        long now = System.currentTimeMillis();
        String body = StandardCharsets.UTF_8.decode(message.getBody()).toString();
        long bornLatency = now - message.getBornTimestamp();
        // 定时消息的"迟到量" = 实际投递时刻 - 声明的投递时刻，和 bornLatency 不是一回事：
        // bornLatency 恒为正（消息总要先产生），迟到量可能是负数（Broker 时钟偏快）。
        String deliverDelay = message.getDeliveryTimestamp()
                .map(target -> " lateBy=" + (now - target) + "ms")
                .orElse("");
        log.info("冒烟 recv topic={} tag={} msgId={} bornLatency={}ms{} body={}",
                message.getTopic(), message.getTag().orElse("-"), message.getMessageId(),
                bornLatency, deliverDelay,
                body.length() > 200 ? body.substring(0, 200) + "..." : body);
        return ConsumeResult.SUCCESS;
    }

    @PreDestroy
    public void shutdown() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("dev 冒烟消费者关闭失败", e);
            }
        }
    }
}
