package com.campusslot.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** 通知记录（异步写）。`(biz_id, channel)` 唯一索引保证幂等。 */
@Data
@TableName("notification")
public class Notification {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String bizId;

    private String studentId;

    private Long resourceId;

    /** SMS / WECOM / MOCK */
    private String channel;

    private String title;

    private String content;

    /** PENDING / SENT / FAILED / DEAD */
    private String status;

    /**
     * <b>已尝试次数（含首次）</b>，不是"重试次数"。
     *
     * <p>{@code retryCount=1} 表示第一次发送就成功（0 次重试）；{@code =3} 表示第 3 次才成功。
     * 写入点在 {@code NotificationRetryJob}：{@code attempt = retryCount + 1}，发送后把 attempt 存回。
     *
     * <p>口径要写死是因为它差一：D9 报表如果把它当"重试次数"求平均，所有值都会虚高 1，
     * 而数据库里 {@code SENT/1} 和 {@code SENT/2} 看起来都像"重试过"。真实含义见上。
     */
    private Integer retryCount;

    /** 下次重试时刻，指数退避；没有它重试器只能全表扫 */
    private LocalDateTime nextRetryAt;

    private String errorMsg;

    private LocalDateTime sentAt;

    private LocalDateTime createdAt;
}
