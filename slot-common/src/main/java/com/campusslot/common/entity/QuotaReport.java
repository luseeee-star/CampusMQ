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

    /** 收到的申领请求数（含重复提交），与 granted 的差额就是被拒的量 */
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
