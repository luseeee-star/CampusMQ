package com.campusslot.app.gateway.ratelimit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * D6 入口限流的配置。
 *
 * <p>{@code enabled} 默认 <b>false</b>：D5 的全部基线数据都是在无限流下测的，
 * 默认打开会让「复现 D5 曲线」变成复现不了 —— 一跑就撞 429，看起来像系统退化了。
 * 需要限流保护时才显式打开（压测脚本会传 {@code -RemoteRateLimitQps}）。
 *
 * <p>{@code qps} 默认 3000 是<b>经验值，不是安全值</b>，别再把它读成"最后一个还舒服的档位"。
 * D5 的「3000 档 P99=145ms」是在 <b>5 秒窗口、表近似为空</b> 的条件下测的；
 * 2026-11-11 用 60 秒稳态 / 48.4 万行重测同一档：P99 = 5,565ms、容量 <b>2,846/s</b> ——
 * <b>已经低于这个闸门</b>。闸门高于容量，意味着开着它每秒仍有约 5% 的请求排不上队，
 * 而排不上不是被拒绝，是<b>等</b>。
 *
 * <p><b>安全闸门待定</b>：要定它得从 1,000 起逐档做 60 秒稳态扫描，每档之前必须排空
 * （积压本身会反噬入口），本轮窗口装不下。在那之前保持 3000 不动 ——
 * 它的准确定位是"入口还能自主决定"的上限，持续的 P99 由消费侧决定，
 * 见 {@code docs/acceptance/2026-11-11/rl-calibration.md} §五。
 */
@Component
@ConfigurationProperties(prefix = "campusslot.gateway.rate-limit")
public class RateLimitProperties {

    /** 总开关。关闭时过滤器直接放行，不做任何匹配判断以外的开销。 */
    private boolean enabled = false;

    /** 长期放行速率（个/秒）。 */
    private int qps = 3000;

    /** 桶容量，瞬时允许透支的量。默认等于 qps（1 秒的量）。 */
    private int burst = 3000;

    /**
     * 需要限流的路径（Ant 风格）。默认只拦申领主链路 ——
     * 浏览、查询、管理端不该被抢名额的洪峰连坐。
     */
    private List<String> includePaths = new ArrayList<>(List.of("/api/claim", "/api/claim/mq"));

    /**
     * 排除路径。dev 端点必须排除：压 ping / drain / reset 被限流，
     * 会得到「压测器没起来」或「账本没追平」这种完全误导的结论。
     */
    private List<String> excludePaths = new ArrayList<>(List.of("/api/claim/dev/**"));

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getQps() {
        return qps;
    }

    public void setQps(int qps) {
        this.qps = qps;
    }

    public int getBurst() {
        return burst;
    }

    public void setBurst(int burst) {
        this.burst = burst;
    }

    public List<String> getIncludePaths() {
        return includePaths;
    }

    public void setIncludePaths(List<String> includePaths) {
        this.includePaths = includePaths;
    }

    public List<String> getExcludePaths() {
        return excludePaths;
    }

    public void setExcludePaths(List<String> excludePaths) {
        this.excludePaths = excludePaths;
    }
}
