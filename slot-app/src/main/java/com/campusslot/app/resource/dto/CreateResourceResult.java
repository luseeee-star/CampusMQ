package com.campusslot.app.resource.dto;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 建活动的返回。
 *
 * <p>{@code rosterRows} 必须回显：名单是分批独立提交的，如果插入在中途失败，
 * 活动会停在 DRAFT 且只有一部分名单——外部只看"接口报错了"是不知道差多少的。
 * 把这个数交给调用方，才能在发布前和导入的条数对一次。
 */
public record CreateResourceResult(
        long resourceId,
        String status,
        long deptId,
        int totalQuota,
        int remain,
        LocalDateTime openAt,
        LocalDateTime closeAt,
        LocalDateTime cancelDeadline,
        int confirmTimeoutSec,
        boolean rosterRequired,
        int collegeRows,
        int rosterRows,
        int rosterDuplicates,
        List<String> warnings
) {
}
