package com.campusslot.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 对账差异记录（P0-8）。分布式系统里 Redis 与 MySQL 必然会有瞬时不一致，
 * 关键不是「永远一致」，而是「不一致时 1 分钟内被发现并可修复」。
 */
@Data
@TableName("reconcile_diff")
public class ReconcileDiff {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long resourceId;

    /** 对账时刻 Redis 中的余量 */
    private Integer redisRemain;

    /** 对账时刻 DB 口径的余量（total_quota - 已发放） */
    private Integer dbRemain;

    /** redisRemain - dbRemain，非 0 即漂移 */
    private Integer drift;

    private String fixAction;

    private LocalDateTime detectedAt;

    /** NULL = 未修复，D9 修完回填。不新增 status 列：两套状态表达同一件事迟早会不一致。 */
    private LocalDateTime fixedAt;

    /**
     * 记录来源：{@code SCAN} 定期巡检（按资源聚合）/ {@code CANCEL} 取消被拒 /
     * {@code TIMEOUT} 超时释放被拒。
     */
    private String source;

    /** 事件级记录才有；巡检聚合记录为 null。 */
    private String studentId;

    /** 事件级记录才有；巡检聚合记录为 null。 */
    private Long recordId;

    /** 判定时的配额上限。扩容后旧记录的这个值可能不等于当前 total_quota，是历史快照。 */
    private Integer totalQuota;
}
