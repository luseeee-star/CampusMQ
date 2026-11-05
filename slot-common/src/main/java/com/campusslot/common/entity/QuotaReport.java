package com.campusslot.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 每次发放的对账报表（G-4 / R-7）。
 *
 * <p>平衡等式：{@code confirmed + released + locked == granted}，且 {@code granted <= total_quota}。
 * 两项都成立才把 {@code balanced} 置 1；只要有偏差就存原值，<b>不允许为了让报表好看去修正数字</b>——
 * 报表的价值在于它能显示出不平，而不是它永远显示平。
 */
@Data
@TableName("quota_report")
public class QuotaReport {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long resourceId;

    private Long deptId;

    private Integer totalQuota;

    /**
     * 成功锁定的请求数。
     *
     * <p>表注释写的是「申请总数（含重复提交）」，但**这个数在库里没有数据源**：
     * 被拒的请求不落 {@code quota_record}，重复提交被 Lua 的幂等键挡在 Redis 层，
     * 两者都不会留下任何一行。所以巡检落表时填的就是记录数，等于 {@link #grantedCnt}。
     *
     * <p>宁可让这一列等于成功数，也不要凭「大概多少人在抢」估一个值——
     * 报表上看不出来的假设，比一个明显的常数危险得多。要真实申请量只能加埋点，
     * 那是另一个决定（会多一次写），不在 D9 范围内。
     */
    private Integer appliedCnt;

    /** 锁定成功数 */
    private Integer grantedCnt;

    private Integer confirmedCnt;

    private Integer releasedCnt;

    /** 仍处于锁定中的数量 */
    private Integer lockedCnt;

    private Integer remain;

    private Boolean balanced;

    private LocalDateTime generatedAt;
}
