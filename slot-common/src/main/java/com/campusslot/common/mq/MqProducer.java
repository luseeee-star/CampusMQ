package com.campusslot.common.mq;

import java.time.Duration;

/** 生产端抽象。让主链路在 MQ 未启用/不可用时仍能装配（降级由实现类决定，不由调用方判断）。 */
public interface MqProducer {

    /** 同步发送。返回消息 ID；失败抛 {@link MqSendException}，调用方据此决定是否回滚预扣。 */
    String send(String topic, String key, String tag, String payload);

    /**
     * 定时消息：不早于 delay 之后投递。
     * 必须走 5.x gRPC 客户端——4.x 客户端只有 18 个固定档位，会把业务逻辑逼成打补丁（需求书 §6.2）。
     */
    String sendDelay(String topic, String key, String tag, String payload, Duration delay);
}
