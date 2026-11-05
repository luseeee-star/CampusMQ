package com.campusslot.common.mq;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * {@link ClaimMessage} 的 JSON 编解码。两个进程共用同一份配置，这是它单独成一个类的原因。
 *
 * <p>如果生产端用 Spring 容器里那个 {@code ObjectMapper}、消费端自己 {@code new} 一个，
 * 两边的 {@code FAIL_ON_UNKNOWN_PROPERTIES}、日期格式、null 处理各不相同。
 * 只要哪天在消息里加一个字段，消费端就会整片抛异常——而异常发生在异步链路里，
 * 表现是「接口全部返回成功，但一条记录都没有」，排查从「哪里报错了」变成「哪里没报错」。
 *
 * <p>{@code FAIL_ON_UNKNOWN_PROPERTIES=false}：新版本生产者多写一个字段时，
 * 旧消费者应该忽略它继续工作（向后兼容），而不是拒收。消息格式的演进方向只能是「加字段」。
 */
public final class ClaimMessageCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private ClaimMessageCodec() {
    }

    public static String encode(ClaimMessage message) {
        try {
            return MAPPER.writeValueAsString(message);
        } catch (Exception e) {
            // 序列化一个纯 record 失败只可能是环境级问题，包装成运行时异常让调用方回补
            throw new IllegalStateException("序列化 ClaimMessage 失败: " + message, e);
        }
    }

    /**
     * 解码。失败抛 {@link IllegalArgumentException}——调用方必须把它和「业务失败」分开：
     * 业务失败要重试，解码失败重试一万次也一样，属于毒消息，只能丢弃 + 告警。
     */
    public static ClaimMessage decode(String json) {
        try {
            return MAPPER.readValue(json, ClaimMessage.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("无法解析 ClaimMessage: "
                    + (json == null ? "null" : abbreviate(json)), e);
        }
    }

    private static String abbreviate(String s) {
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }
}
