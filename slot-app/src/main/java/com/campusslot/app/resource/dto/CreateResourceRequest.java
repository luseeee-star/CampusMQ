package com.campusslot.app.resource.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 建活动（G-1 + G-2 并入）。
 *
 * <p><b>为什么名单并进建活动，而不是另开一个"配置资格"接口</b>：分开之后就会出现
 * "活动建好了、名单还没导"的中间态，而那个状态和"导到一半失败"在外部看起来完全一样。
 * 并进来之后，最坏情况是这次调用整体失败、事务回滚，不会留下一个半配置的活动。
 * 名单本身仍然<b>不在同一个事务里</b>（几千上万行持锁太久），所以真正兜底的是发布前的条数校验。
 *
 * <p><b>{@code deptId} 定成必填，不给默认值</b>：{@code dept} 表只有两行，
 * 默认值会让所有接口建出来的活动全落同一个部门；而 resource 包的口径是
 * "所有查询必须带 dept_id 维度"——只有一个取值的维度不叫维度。
 *
 * <p><b>{@code confirmTimeoutSec} 和 {@code rosterRequired} 可以不传</b>：库里这两列有默认值
 * （1800 / 0）。其余 {@code quota_resource} 的 NOT NULL 列都没有默认值，缺了就是 500，
 * 所以在 DTO 上直接卡住，别让请求走到 SQL 才报错。
 */
public record CreateResourceRequest(

        @NotNull(message = "deptId 必填")
        Long deptId,

        @NotBlank(message = "title 必填")
        @Size(max = 200, message = "title 不能超过 200 字")
        String title,

        @Size(max = 1000, message = "description 不能超过 1000 字")
        String description,

        @NotNull(message = "totalQuota 必填")
        @Min(value = 0, message = "totalQuota 不能为负数")
        Integer totalQuota,

        @NotNull(message = "openAt 必填（ISO 格式，如 2026-11-10T09:00:00）")
        LocalDateTime openAt,

        @NotNull(message = "closeAt 必填（ISO 格式，如 2026-11-10T18:00:00）")
        LocalDateTime closeAt,

        /** 取消截止时刻；不传 = 关闭前都可取消。 */
        LocalDateTime cancelDeadline,

        @Min(value = 1, message = "confirmTimeoutSec 至少 1 秒")
        Integer confirmTimeoutSec,

        /** true = 只允许名单内学号申领。传 false/不传时不要带 roster，带了会拒绝（名单不会生效）。 */
        Boolean rosterRequired,

        @Valid
        List<CollegeQuotaItem> colleges,

        @Valid
        List<RosterItem> roster
) {
}
