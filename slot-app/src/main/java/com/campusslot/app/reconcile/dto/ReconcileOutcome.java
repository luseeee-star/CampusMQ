package com.campusslot.app.reconcile.dto;

/**
 * 单个活动一次巡检的结论（SYS-3）。
 *
 * <p><b>为什么把两套余量都摊开</b>：{@code dbRemain} 由记录反推
 * （{@code total_quota − LOCKED − CONFIRMED}），{@code dbRemainColumn} 是
 * {@code quota_resource.remain} 这一列的原值。正常情况两者相等；不等说明有人绕过接口改过列，
 * 或者某次扣减没有同步它。只报一个数就分不出"账平"和"列被改过"。
 *
 * <p>{@code action} 取值：
 * <ul>
 *   <li>{@code NONE} —— 差值为 0，什么都没写</li>
 *   <li>{@code DIFF} —— 写了 {@code reconcile_diff}（可能同时写了 {@code quota_report}）</li>
 *   <li>{@code STOCK_KEY_MISSING} —— 库存 key 不存在，<b>没有写 diff</b>，见下</li>
 *   <li>{@code REPORT_FINAL} —— 活动已关闭，补一份可归档的终版报表</li>
 *   <li>{@code SKIPPED} —— 草稿活动，还没有库存 key，不做比对</li>
 * </ul>
 *
 * <p><b>key 缺失为什么不写成 diff</b>：{@code reconcile_diff.redis_remain} 是 {@code NOT NULL}，
 * 填 0 会造出 {@code drift = −total_quota} 这种记录——看着像"整个活动的名额都丢了"，
 * 实际是"这个活动的库存 key 没了"。两者的处置完全不同（一个是核账，一个是重新灌 key），
 * 混进同一个字段只会让人往错的方向查。所以这里单独给一个布尔位 + ERROR 日志，
 * 由 {@code /progress} 的 {@code stockKeyMissing} 和本字段共同承担发现职责。
 *
 * @param dbIssued           当前仍有效的已发数（LOCKED + CONFIRMED）
 * @param dbRemain           由记录反推的余量 = totalQuota − dbIssued
 * @param dbRemainColumn     {@code quota_resource.remain} 原值，用来发现"列被绕过改过"
 * @param redisRemain        Redis 库存，null 表示 key 不存在（此时 drift 也为 null）
 * @param severity           null / INFO / WARN。负差异第一次是 INFO，连续第二个周期升 WARN；
 *                           正差异第一次就是 WARN（Redis 比 DB 多只有人工改或重复回补两种成因）
 * @param consecutiveNegative 本次是否判定为"连续第二个周期为负"
 * @param reportWritten      本次是否写了 {@code quota_report}
 */
public record ReconcileOutcome(
        long resourceId,
        String resourceStatus,
        int totalQuota,
        int records,
        int locked,
        int confirmed,
        int released,
        int dbIssued,
        int dbRemain,
        Integer dbRemainColumn,
        Integer redisRemain,
        Integer drift,
        boolean stockKeyMissing,
        String action,
        String severity,
        boolean consecutiveNegative,
        boolean reportWritten,
        String note) {
}
