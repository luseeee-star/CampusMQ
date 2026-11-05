package com.campusslot.common.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MQ 未启用时的占位实现：只打日志、不落 Broker。
 *
 * <p><b>它存在的唯一理由是让 D0/D1 的「没有中间件也能起服务」成立。</b>
 * 一旦进入 D3 主链路联调与压测，必须把 campusslot.mq.enabled 打开——
 * 否则申领会「看起来成功」，而 E1/E2 的数据全部作废。
 */
@Configuration
public class MqFallbackConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MqFallbackConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(MqProducer.class)
    public MqProducer noopMqProducer() {
        return new MqProducer() {
            @Override
            public String send(String topic, String key, String tag, String payload) {
                log.warn("[MQ-DISABLED] 消息未发往 Broker, topic={}, key={}, bytes={}",
                        topic, key, payload == null ? 0 : payload.length());
                return "noop-" + key;
            }

            @Override
            public String sendDelay(String topic, String key, String tag, String payload, java.time.Duration delay) {
                log.warn("[MQ-DISABLED] 定时消息未发往 Broker, topic={}, key={}, delay={}",
                        topic, key, delay);
                return "noop-" + key;
            }
        };
    }
}
