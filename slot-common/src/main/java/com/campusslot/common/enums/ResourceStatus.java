package com.campusslot.common.enums;

public enum ResourceStatus {

    /** 草稿，部门端可编辑，学生端不可见。 */
    DRAFT,

    /** 已发布，到 open_at 自动开放申领。 */
    PUBLISHED,

    /** 报名/申领窗口已关闭。 */
    CLOSED,

    /** 已下架。 */
    OFFLINE
}
