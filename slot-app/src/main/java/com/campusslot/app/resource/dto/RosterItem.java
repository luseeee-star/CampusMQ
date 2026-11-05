package com.campusslot.app.resource.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * 名单里的一行。
 *
 * <p><b>{@code collegeCode} 在这里没加 {@code @NotBlank}</b>，因为它是否必填取决于请求级别的
 * {@code rosterRequired}——单行注解表达不了"只有当活动要名单时才必填"。真正的判据放在
 * {@code ResourceAdminService}，因为那里才能给出"第几行、哪个学号"的错误提示；
 * 只说"collegeCode 不能为空"，运营对着三千行名单不知道改哪一行。
 *
 * <p>它<b>不是可选字段</b>：{@code QuotaRedisSeeder#seedRoster} 会静默跳过学院编码为空的行，
 * 一整个名单灌完 0 个 key，发布照样成功，抢的时候全场 1005。必填的道理在
 * {@code ResourceMeta#needRosterCheck()} 的注释里写全了。
 */
public record RosterItem(
        @NotBlank(message = "名单行的 studentId 不能为空")
        String studentId,

        String collegeCode
) {
}
