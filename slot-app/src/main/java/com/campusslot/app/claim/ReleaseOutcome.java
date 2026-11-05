package com.campusslot.app.claim;

import com.campusslot.common.api.ErrorCode;

/**
 * 一次「释放」的结果（S-4 取消 / SYS-1 超时）。
 *
 * <p>与 {@link ConfirmOutcome} 分开：释放有三个确认没有的返回位——
 * <b>释放原因</b>（{@code CANCEL} / {@code TIMEOUT}，前端提示与对账 source 都用它）、
 * <b>余量是否已在 Redis 侧对齐</b>（{@code synced=false} 表示走了对账兜底），
 * 以及<b>本次是否真的做了状态迁移</b>（{@code replayed=true} 表示记录已经是终态）。
 * 后两者不上响应体（前端无法处理，也不该处理），只进日志与指标。
 *
 * <p><b>{@code releaseReason} 必须是参数，不能写死成 CANCEL。</b>SYS-1 复用同一条
 * {@code releaseIfLocked} 路径，只是 reason 不同；写死会让超时释放的记录在
 * {@code quota_record.release_reason} 里全部显示成 CANCEL，D12 做报表时按 reason 分组
 * 会把"超时"读成"学生主动取消"——一个不会报错的错数。
 *
 * <p>三种成功形态必须区分清楚：
 * <ul>
 *   <li>{@code ok} —— 本次真的完成了 LOCKED → RELEASED</li>
 *   <li>{@code replay} —— 记录已经是 RELEASED，幂等返回 200（重复点击不是错误）</li>
 *   <li>{@code ok(synced=false)} —— 状态迁移成功但 Redis 回补被拒/失败，已落对账</li>
 * </ul>
 * 第三种对用户仍然是成功：他的名额确实释放了，报失败只会让他重试，
 * 而重试会走幂等分支，什么也不会改变。
 */
public record ReleaseOutcome(boolean success,
                             ErrorCode error,
                             String status,
                             String releaseReason,
                             boolean replayed,
                             Integer remain,
                             boolean synced,
                             String note) {

    /** 本次完成 LOCKED → RELEASED，Redis 余量已对齐。 */
    public static ReleaseOutcome ok(String reason, String status, Integer remain) {
        return new ReleaseOutcome(true, null, status, reason, false, remain, true, null);
    }

    /**
     * 完成状态迁移，但 Redis 侧靠对账兜底（{@code synced=false}）。
     *
     * <p>对用户仍是成功：名额确实释放了。报失败只会让他重试，而重试走幂等分支，
     * 什么也不会改变——徒增一次请求和一条困惑。
     */
    public static ReleaseOutcome okPending(String reason, String status, Integer remain, String note) {
        return new ReleaseOutcome(true, null, status, reason, false, remain, false, note);
    }

    /**
     * 幂等命中：这条记录已经是 RELEASED（且是同一个 reason 释放的）。
     *
     * <p><b>200，不是 409。</b>「已经是你要的状态」不是错误——重复点击弹红框，
     * 用户只会以为取消失败再点一次。409 只留给「目标状态互斥」，即取消一条已确认的记录。
     * 返回体带 {@code status} 与 {@code releaseReason}，前端据此渲染"已于何时因何释放"。
     */
    public static ReleaseOutcome replay(String releaseReason) {
        return new ReleaseOutcome(true, null, "RELEASED",
                releaseReason == null ? "CANCEL" : releaseReason, true, null, true, null);
    }

    /**
     * SYS-1 扫到了一条记录，但更新时它已经不是 LOCKED —— 别人先处理了。
     *
     * <p><b>为什么这里能确定是 RELEASED 而不是 CONFIRMED</b>：扫描条件是
     * {@code expire_at <= now}，而确认要求 {@code expire_at > now}，同一条记录不可能
     * 先被确认再被扫到。能赢的只有学生取消（取消侧刻意不判 {@code expire_at}）
     * 或另一个实例的扫描。所以这不是"猜一个状态"，是条件互斥推出来的结论。
     *
     * <p>{@code replayed=true} 让定时任务把它记成"跳过"而不是"释放了"——
     * 追平速度要靠这个数判断，把别人干的活算成自己的会高估扫表吞吐。
     */
    public static ReleaseOutcome alreadyReleased(String releaseReason) {
        return new ReleaseOutcome(true, null, "RELEASED", releaseReason, true, null, true, null);
    }

    public static ReleaseOutcome fail(ErrorCode error) {
        return new ReleaseOutcome(false, error, null, null, false, null, true, null);
    }

    public static ReleaseOutcome fail(ErrorCode error, String note) {
        return new ReleaseOutcome(false, error, null, null, false, null, true, note);
    }
}
