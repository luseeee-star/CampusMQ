package com.campusslot.common.mq;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** RocketMQ 5.x 接入配置。endpoints 指向 proxy 的 gRPC 端口，不是 namesrv 的 9876。 */
@Data
@Component
@ConfigurationProperties(prefix = "campusslot.mq")
public class MqProperties {

    /**
     * 本地无中间件时置 false，应用仍能启动（D0/D1 环境自检用）。
     * 生产与压测必须为 true，否则主链路会在「假装成功」的状态下跑，数据全部无效。
     */
    private boolean enabled = false;

    /** RocketMQ 5.x Proxy gRPC 端点，例如 127.0.0.1:8081 */
    private String endpoints = "127.0.0.1:8081";

    /** 未开启 ACL 时留空 */
    private String accessKey;

    private String secretKey;

    /** 同步发送超时（毫秒）。同步发送是为了让「返回成功」与「消息已进 Broker」是同一件事。 */
    private long sendTimeoutMs = 3000;
}
