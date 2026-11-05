package com.campusslot.app.claim;

import com.campusslot.common.constant.AppTime;

/**
 * 资源元信息的只读快照，申领链路上唯一需要知道的「活动配置」。
 *
 * <p><b>为什么要有这个东西</b>：D2 的 {@code ClaimService} 每来一个请求就
 * {@code SELECT * FROM quota_resource WHERE id = ?} 一次。8000 QPS 下就是 8000 次
 * 单行主键查询压在 64 个连接的池子上——这不是"小优化"，这是 D3 要搬掉的两次 DB 往返之一
 * （另一次是 INSERT + UPDATE）。不搬它，D3 的 RT 对比里永远混着一次主键查询，
 * 「异步落库到底省了多少」这个问题就答不干净。
 *
 * <p><b>时间用 epoch 毫秒而不是 LocalDateTime</b>：这份结构要序列化进 Redis，
 * 而 {@code LocalDateTime} 在 JSON 里不带时区，换个容器时区就整体偏 8 小时。
 * 存毫秒，读出来按 {@link AppTime#ZONE} 还原，只有一处转换，不会有两处各按各的理解。
 *
 * <p><b>缓存里不放「是否开放」这个结论</b>：放的是 openAt / closeAt 两个时刻，
 * 每次请求现算。反过来的话，活动刚开放那一秒打进来的请求会命中 5 秒前算好的
 * {@code isOpen=false}，表现为"整点抢不到，过几秒才能抢"——这正好废掉秒杀场景。
 * 结论会过期，原始时刻不会。
 */
public record ResourceMeta(long id,
                           long deptId,
                           int totalQuota,
                           String status,
                           long openAtMs,
                           long closeAtMs,
                           int confirmTimeoutSec,
                           boolean rosterRequired,
                           boolean collegeQuotaEnabled) {

    /** 需求书 §4.2：PUBLISHED 且在 [openAt, closeAt] 内才放行。 */
    public boolean isOpen(long nowMs) {
        if (!"PUBLISHED".equals(status)) {
            return false;
        }
        if (nowMs < openAtMs) {
            return false;
        }
        return nowMs <= closeAtMs;
    }

    /**
     * 学院配额是否生效 = 资源要名单 + 这场发放配了学院行 + 全局开关打开。
     *
     * <p>三个条件缺一不可。少判「资源要名单」就会出现：没配名单的活动也去查
     * {@code quota:roster:*}，查不到就判「不在名单内」，整场活动没人能申领。
     * 少判「有没有学院行」则会让「开了开关但没配行」的活动把所有人拒成 1005。
     */
    public boolean useCollegeQuota() {
        return rosterRequired && collegeQuotaEnabled;
    }
}
