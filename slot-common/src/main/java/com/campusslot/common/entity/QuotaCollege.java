package com.campusslot.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 学院配额（G-1「按学院设配额」）。
 *
 * <p>这是「学院配额」不是「学院名额占比」：remain 扣到 0 之后该学院的请求必须被拒，
 * 而总名额可能还剩。所以对外的文案要能区分「全场满了」和「你学院满了」——
 * 说「名额已满」但页面还显示有剩余，是最容易招投诉的一种不一致。
 */
@Data
@TableName("quota_college")
public class QuotaCollege {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long resourceId;

    private String collegeCode;

    private Integer quota;

    private Integer remain;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}
