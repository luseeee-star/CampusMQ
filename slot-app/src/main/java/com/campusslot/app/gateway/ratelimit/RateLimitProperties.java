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
 * <p>{@code qps} 默认 3000 不是拍脑袋：D5 开环实测 3000 档 P99=145ms、处理能力仍占输入的 98%，
 * 4000 档开始积压（76%）并越过 200ms。**3000 是这台机器上最后一个还舒服的档位**，
 * 把闸门设在这里，等于让系统在被打爆之前主动拒绝，而不是被压垮后全员超时。
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
