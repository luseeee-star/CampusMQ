package com.campusslot.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 资源定义（一场发放）。remain 与 total_quota 的差额即已发放数，对账以 DB 为准（Redis 只是镜像）。
 *
 * <p>v2.0 去掉了 {@code mode}（发放模式）与 {@code draw_at}（开奖时刻）：
 * 需求书 §9 明确「不做抽签 / 摇号」，留着这两个字段会让人以为还有另一条链路没实现。
 * 新增 {@code cancel_deadline}：S-4 要求「在取消截止时间前主动取消」，没有这个时刻，
 * 「能不能取消」就只能由代码里的某个 if 决定，运营没法按活动配置。
 */
@Data
@TableName("quota_resource")
public class QuotaResource {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long deptId;

    private String title;

    private String description;

    private Integer totalQuota;

    /** 剩余名额，绝对防线由 DB 条件更新保证：UPDATE ... WHERE remain > 0 */
    private Integer remain;

    /** DRAFT / PUBLISHED / CLOSED / OFFLINE */
    private String status;

    /** 开放时刻，早于它一律拒绝（§4.2） */
    private LocalDateTime openAt;

    private LocalDateTime closeAt;

    /** 取消截止时刻；为空表示关闭前都可取消 */
    private LocalDateTime cancelDeadline;

    /** 确认时限（秒），默认 1800 = 30 分钟 */
    private Integer confirmTimeoutSec;

    /** 是否需要资格名单（P4：资格名单前置） */
    private Boolean rosterRequired;

    private Integer version;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}
