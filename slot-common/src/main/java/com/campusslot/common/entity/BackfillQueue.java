package com.campusslot.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 补位队列。名额释放后不重新开放抢号（否则又是一次流量冲击），
 * 而是从队列按 seq 顺序定向推送（需求书 §4.3 / R-6）。
 *
 * <p>定向通知必须带有效期：没有 expire_at 的队列，第一个被通知的人不响应就永远卡在那儿，
 * 后面的名额全被一个人堵死，而且从监控上看「队列在动、就是发不出去」。
 */
@Data
@TableName("quota_backfill_queue")
public class BackfillQueue {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long resourceId;

    private String studentId;

    /** 入队序号，保证「按序定向补发」 */
    private Long seq;

    /** WAITING / NOTIFIED / EXPIRED / GRANTED */
    private String status;

    private LocalDateTime notifiedAt;

    /** 定向通知的有效期，过期让位给下一位 */
    private LocalDateTime expireAt;

    private LocalDateTime createdAt;
}
