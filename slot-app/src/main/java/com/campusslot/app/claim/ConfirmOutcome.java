package com.campusslot.app.claim;

import com.campusslot.common.api.ErrorCode;

/**
 * 一次「确认」的结果（S-3）。
 *
 * <p>与 {@link ClaimOutcome} 分开而不是复用它：两者的状态语义不同。申领的首次结果恒为
 * {@code LOCKED}（R-3 要求重放逐字节一致），确认的目标状态恒为 {@code CONFIRMED}，
 * 而"重放"在这里的含义是"你已经确认过了"——不是错误，但也不是一次新的状态迁移。
 * 塞进一个 record 里会让两个语义共用同一个 {@code note} 字段，最后谁都不敢改。
 *
 * <p><b>{@code replayed} 同样不上响应体，只走 HTTP 头 {@code X-Confirm-Replay}</b>，
 * 理由与申领一致：前端拿它做提示（"已确认过"），但响应体的字段一旦带了这个，
 * 就成了对外的契约，往后想改要兼容两个字段。
 */
public record ConfirmOutcome(boolean success,
                             ErrorCode error,
                             String status,
                             boolean replayed,
                             String note) {

    /** 本次调用真的完成了 LOCKED → CONFIRMED。 */
    public static ConfirmOutcome ok() {
        return new ConfirmOutcome(true, null, "CONFIRMED", false, null);
    }

    /**
     * 幂等命中：这条记录已经是 CONFIRMED。
     *
     * <p><b>这是 200，不是 409。</b>「已经是你要的状态」不是错误——前端重复点击时弹一个红框，
     * 学生只会以为确认失败了再点一次。409 只留给「目标状态与当前状态互斥」，
     * 也就是取消一条已确认的记录、或者确认一条已释放的记录。
     */
    public static ConfirmOutcome replay() {
        return new ConfirmOutcome(true, null, "CONFIRMED", true, null);
    }

    public static ConfirmOutcome fail(ErrorCode error) {
        return new ConfirmOutcome(false, error, null, false, null);
    }

    public static ConfirmOutcome fail(ErrorCode error, String note) {
        return new ConfirmOutcome(false, error, null, false, note);
    }
}
