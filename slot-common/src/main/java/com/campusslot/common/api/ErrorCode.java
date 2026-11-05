package com.campusslot.common.api;

/**
 * 业务错误码。
 *
 * <p>需求书 §4.2 要求「超量/无余量必须有明确提示，不得静默失败」（R-8），
 * §7 NFR-2 要求「限流拒绝不计入失败率」。因此**预期内**的拒绝与系统级错误必须分区间：
 * 1xxx 参数与身份、2xxx 名额与状态、3xxx 限流（不计入失败率）、9xxx 系统级。
 *
 * <p>v2.0 删除了 {@code ACCOUNT_BLOCKED}：§9 明确不做账号封禁 / 黑名单，
 * 留着一个永远不会被抛出的错误码，只会让人以为系统有风控模块。
 */
public enum ErrorCode {

    // ===== 1xxx 参数与身份 =====
    PARAM_INVALID(1001, "参数不合法"),
    STUDENT_NOT_LOGIN(1002, "未识别到学生身份"),
    RESOURCE_NOT_FOUND(1003, "资源不存在"),
    RESOURCE_NOT_OPEN(1004, "活动未开放或已结束"),
    NOT_ELIGIBLE(1005, "不在本次发放资格名单内"),

    // ===== 2xxx 名额与状态 =====
    QUOTA_FULL(2001, "名额已满"),
    COLLEGE_QUOTA_FULL(2002, "你所在学院的名额已满"),
    RECORD_NOT_FOUND(2003, "申领记录不存在"),
    RECORD_STATUS_ILLEGAL(2004, "当前状态不允许该操作"),
    CANCEL_DEADLINE_PASSED(2005, "已过取消截止时间，无法取消"),
    /**
     * 活动当前状态不允许该操作（管理端）。
     *
     * <p>与 {@link #RECORD_STATUS_ILLEGAL} 分开，是因为两者的处置完全不同：
     * 记录状态非法是学生点错了按钮，活动状态非法是运营把已下架的活动又发布了一次。
     * 混用一个码，排查时得先去读消息才能分清是谁的问题。
     */
    RESOURCE_STATUS_ILLEGAL(2007, "当前活动状态不允许该操作"),
    /**
     * 重复申领。
     *
     * <p>D2 阶段只能回这句话，因为简化版 Lua 的幂等键只存了 "1"，还原不出首次结果。
     * D4 会把它改成「原样返回首次结果」（R-3）——那时这条错误码在正常路径上不再出现，
     * 只在「Redis 幂等键已过期但 DB 已有记录」的兜底路径上保留。
     */
    ALREADY_CLAIMED(2006, "你已申领过该资源"),

    // ===== 3xxx 限流（主动拒绝，不计入 NFR-2 失败率）=====
    RATE_LIMITED(3001, "当前人数过多，请稍后再试"),
    SIGN_INVALID(3002, "签名校验失败"),
    REPLAY_REJECTED(3003, "重复提交，请勿频繁操作"),

    // ===== 9xxx 系统级 =====
    SYSTEM_BUSY(9001, "系统繁忙，请稍后重试"),
    REDIS_UNAVAILABLE(9002, "系统繁忙，请稍后重试"),
    MQ_UNAVAILABLE(9003, "系统繁忙，请稍后重试"),
    DEGRADED(9004, "当前仅支持查看"),
    /** 库存 key 缺失：P0 告警，绝不能降级成「名额已满」——那会让学生以为名额没了，实际是系统坏了。 */
    STOCK_CACHE_MISSING(9005, "系统繁忙，请稍后重试");

    private final int code;
    private final String message;

    ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    /** 是否属于「主动拒绝」（限流等），用于 NFR-2 的失败率统计口径。 */
    public boolean isActiveRejection() {
        return code >= 3000 && code < 4000;
    }
}
