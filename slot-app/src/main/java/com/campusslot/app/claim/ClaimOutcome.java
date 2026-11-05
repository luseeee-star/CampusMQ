package com.campusslot.app.claim;

import com.campusslot.common.api.ErrorCode;

/**
 * 一次申领的结果。
 *
 * <p>用 record 而不是直接返回 {@code R<Map>}：Service 不该知道自己被 HTTP 包了一层，
 * 否则 D3 接 MQ、D8 接状态机的时候，每加一个分支都得同时改序列化格式。
 *
 * <p><b>{@code replayed} 不上响应体，只用于日志与监控。</b>
 * R-3 要求「重复请求返回首次请求的结果」，§4.2 更直接写「而不是『你已参与过』」——
 * 也就是说重放的 HTTP 响应必须与首次<b>逐字节相同</b>。往里塞一个
 * {@code "replayed": true} 就把这条需求破掉了：前端会据此显示「你已申领过」，
 * 于是又回到了需求书明确反对的那个交互。
 *
 * <p>要区分首次与重放，看 HTTP 头 {@code X-Claim-Replay} 或日志，不要动响应体。
 */
public record ClaimOutcome(boolean success,
                           ErrorCode error,
                           String status,
                           Integer remain,
                           boolean replayed,
                           String note) {

    /** 首次申领成功。status 恒为 LOCKED（确认是 D8 的 S-3）。 */
    public static ClaimOutcome ok(int remain) {
        return new ClaimOutcome(true, null, "LOCKED", remain, false, null);
    }

    /**
     * 幂等命中：原样返回首次结果（R-3）。
     *
     * <p>{@code remain} 来自幂等键里存的「首次扣减后的余量」，所以它和首次响应里的
     * 那个数字是同一个值，不是"现在的余量"。这一点决定了幂等键的 value 不能是 {@code "1"}。
     */
    public static ClaimOutcome replay(int remain) {
        return new ClaimOutcome(true, null, "LOCKED", remain, true, null);
    }

    public static ClaimOutcome fail(ErrorCode error) {
        return new ClaimOutcome(false, error, null, null, false, null);
    }

    public static ClaimOutcome fail(ErrorCode error, String note) {
        return new ClaimOutcome(false, error, null, null, false, note);
    }
}
