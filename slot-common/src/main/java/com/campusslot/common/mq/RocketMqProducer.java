package com.campusslot.common.mq;

import com.campusslot.common.constant.MqTopic;
import jakarta.annotation.PreDestroy;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * RocketMQ 5.x gRPC 生产者。
 *
 * <p>为什么是同步发送（§3.2 的取舍）：sendOneway 能把响应压到 1ms 以下，但 Broker 一抖就会出现
 * 「学生看到已锁定，消息其实丢了」——这是用可靠性换 2ms，不做。
 */
@Component
@ConditionalOnProperty(prefix = "campusslot.mq", name = "enabled", havingValue = "true")
public class RocketMqProducer implements MqProducer {

    private static final Logger log = LoggerFactory.getLogger(RocketMqProducer.class);

    private final MqProperties properties;
    private final ClientServiceProvider provider;
    private final Producer producer;

    public RocketMqProducer(MqProperties properties) throws ClientException {
        this.properties = properties;
        this.provider = ClientServiceProvider.loadService();

        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder()
                .setEndpoints(properties.getEndpoints())
                .setRequestTimeout(Duration.ofMillis(properties.getSendTimeoutMs()));

        if (properties.getAccessKey() != null && !properties.getAccessKey().isBlank()) {
            SessionCredentialsProvider credentials = new StaticSessionCredentialsProvider(
                    properties.getAccessKey(), properties.getSecretKey());
            builder.setCredentialProvider(credentials);
        }

        this.producer = provider.newProducerBuilder()
                .setClientConfiguration(builder.build())
                // 5.x 客户端的 setTopics 是可变参数，不是 List —— 这个坑不踩一次不会知道。
                // 这里必须列全所有会发的 Topic（3 个），少一个就会在运行时抛「topic 未在 producer 中声明」
                .setTopics(
                        MqTopic.QUOTA_CLAIMED,
                        MqTopic.QUOTA_RELEASE_DELAY,
                        MqTopic.QUOTA_BACKFILL)
                .build();

        log.info("RocketMQ producer ready, endpoints={}", properties.getEndpoints());
    }

    @Override
    public String send(String topic, String key, String tag, String payload) {
        return doSend(topic, key, tag, payload, null);
    }

    @Override
    public String sendDelay(String topic, String key, String tag, String payload, Duration delay) {
        return doSend(topic, key, tag, payload, Instant.now().plus(delay));
    }

    private String doSend(String topic, String key, String tag, String payload, Instant deliverAt) {
        MessageBuilder messageBuilder = provider.newMessageBuilder()
                .setTopic(topic)
                .setKeys(key)
                .setBody(payload.getBytes(StandardCharsets.UTF_8));
        if (tag != null && !tag.isBlank()) {
            messageBuilder.setTag(tag);
        }
        if (deliverAt != null) {
            // 5.x gRPC 客户端的定时消息只接受绝对毫秒时间戳，不接受 Instant
            messageBuilder.setDeliveryTimestamp(deliverAt.toEpochMilli());
        }
        Message message = messageBuilder.build();

        try {
            SendReceipt receipt = producer.send(message);
            return receipt.getMessageId().toString();
        } catch (ClientException e) {
            throw new MqSendException("send failed, topic=" + topic + ", key=" + key, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        try {
            producer.close();
        } catch (Exception e) {
            log.warn("producer close failed", e);
        }
    }
}
