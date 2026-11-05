package com.campusslot.app.resource.dto;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 发放进度（G-3），管理端一次看全一个活动的三套账。
 *
 * <p><b>为什么不是一个余量数字</b>：这个系统里"还剩多少"有三个互不相等的答案，
 * 分开列出来才看得出问题出在哪一层：
 * <ul>
 *   <li>{@code dbRemain} —— 数据库看还剩多少，异步链路里它<b>滞后</b>于 Redis（消息还没落库）；</li>
 *   <li>{@code redisRemain} —— 学生实际能抢到的余量，是抢名额那一刻唯一的真相；</li>
 *   <li>{@code expectRemain} —— 由记录反推：总名额减去还在有效期内的（锁定 + 确认）。
 *       它和 {@code dbRemain} 不等，说明 {@code remain} 这一列被绕过改过。</li>
 * </ul>
 * 三者相等才是真平；只报一个数的话，看的人无法区分"发完了"和"账坏了"。
 *
 * <p>{@code drift = redisRemain − dbRemain}：> 0 是在途消息（正常，压测刚停时必然如此），
 * < 0 是 DB 多扣了（只有回补撞车或手工改 Redis 两种成因），必须进对账。
 *
 * <p><b>{@code dbBalanced} 这个名字里的 db 不能省</b>：它只说"数据库自己能不能对上"，
 * 不含 Redis 那一侧。库存 key 被删掉时 {@code dbBalanced} 仍然是 true、{@code redisMatchesDb}
 * 是 false——两个字段一起看才知道系统到底好不好。叫成 {@code balanced} 会让人在
 * 页面显示"账平"的时候以为没事。
 *
 * @param stockKeyMissing 库存 key 不存在。它比余量为 0 严重得多：抢占时
 *                        {@code claim.lua} 读不到 key 会返回 {@code -3}「系统繁忙」，
 *                        表现成"活动挂了"而不是"发完了"，所以单独给一个布尔位。
 * @param rosterKeys      Redis 里实际存在的名单 key 数。和 {@code rosterRows} 不等就是
 *                        漏灌——漏灌是静默的（学生被判成不在名单内，1005），只能靠这两个数对出来。
 */
public record ProgressView(
        long resourceId,
        String title,
        String status,
        long deptId,
        boolean rosterRequired,
        LocalDateTime openAt,
        LocalDateTime closeAt,
        int totalQuota,
        int dbRemain,
        Integer redisRemain,
        boolean stockKeyMissing,
        int expectRemain,
        Integer drift,
        boolean dbBalanced,
        boolean redisMatchesDb,
        int locked,
        int confirmed,
        int released,
        int releasedByTimeout,
        int releasedByCancel,
        int rosterRows,
        int rosterKeys,
        int collegeRows,
        List<CollegeProgress> colleges,
        LocalDateTime now
) {
}
