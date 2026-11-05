package com.campusslot.app.resource.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * 一个学院的配额（G-1）。
 *
 * <p>{@code quota} 和 {@code remain} 建行时相等——这里是"新建"用的入参，
 * 不接受外部指定 {@code remain}：让它可传，就能建出一个"剩下 5 个但配额 20"的活动，
 * 发布后余量与配额立刻对不上，而两边都"合法"，查起来极慢。
 */
public record CollegeQuotaItem(
        @NotBlank(message = "学院编码不能为空")
        String collegeCode,

        @NotNull(message = "学院配额不能为空")
        @Min(value = 0, message = "学院配额不能为负数")
        Integer quota
) {
}
