package com.campusslot.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 申领记录。`(resource_id, student_id)` 唯一索引是幂等的最终防线（R-2 一人一条）：
 * 重复投递、重试、用户狂点都靠它兜住。
 *
 * <p>v2.0 去掉了 {@code redeemedAt} / {@code redeemCode} 与 REDEEMED 状态：
 * 需求书 §9 明确「现场核销由一卡通 / 门禁承担，本系统只出名单」。
 * 加回这两个字段就等于承诺了一个不做、也没法验收的功能。
 */
@Data
@TableName("quota_record")
public class QuotaRecord {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long resourceId;

    private Long deptId;

    private String studentId;

    /** LOCKED（已锁定） / CONFIRMED（已确认） / RELEASED（已释放） */
    private String status;

    /** 来源：FCFS（抢号） / BACKFILL（补位） */
    private String source;

    private LocalDateTime claimAt;

    /** 确认截止时刻 = claimAt + confirmTimeout */
    private LocalDateTime expireAt;

    private LocalDateTime confirmedAt;

    private LocalDateTime releasedAt;

    /** TIMEOUT / CANCEL / SYSTEM —— 释放原因必须留痕，否则对账时分不清「系统超时」与「学生自己取消」 */
    private String releaseReason;

    private Integer version;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}
