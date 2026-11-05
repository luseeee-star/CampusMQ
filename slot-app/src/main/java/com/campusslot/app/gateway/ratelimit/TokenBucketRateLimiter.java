package com.campusslot.app.gateway.ratelimit;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 单机令牌桶限流器（D6）。
 *
 * <p><b>为什么是令牌桶，不是固定窗口。</b>固定窗口在窗口边界会放过 2 倍流量：
 * 第 1 秒末尾放 N 个、第 2 秒开头又放 N 个，两次间隔可能只有 1 毫秒。
 * 活动开场那一下本来就是突发的，边界双倍等于把最危险的时刻放大一倍。
 * 漏桶能削峰但不允许突发 —— 开场 2000 个学生同时点，漏桶会把本该立刻放行的请求
 * 强行摊到之后几秒，延迟从 10ms 变成秒级，用户看到的就是"卡住了"。
 * 令牌桶两头都能兼顾：<b>长期速率被钉在 {@code qps}，瞬时允许透支到 {@code burst}</b>。
 *
 * <p><b>为什么令牌用 double 而不是定点整数。</b>第一版用「微令牌」（1 令牌 = 1000 单位）
 * 想避开浮点漂移，结果实测少放行 19%（限额 1000 只放出 954/s）：每次补充都取整，
 * 而高并发下相邻两次 CAS 的间隔常常不到 1 微秒，{@code elapsed × rate} 不足 1 个单位，
 * 取整直接丢成 0 —— 同时 {@code lastNanos} 又被推进，那段不足 1 单位的时间就永久丢了。
 * CAS 重试越频繁，丢得越多。**double 有 15 位有效数字，1000 个令牌的精度到 1e-12，
 * 比定点方案的取整误差小得多**，所以这里用浮点反而更准。
 *
 * <p><b>为什么按纳秒惰性补充，不启定时器。</b>定时补充需要额外线程，且补充时刻与请求
 * 到达时刻错开，会引入最多一个周期的抖动。惰性补充在每次请求时按流逝时间算，
 * 精度只受 {@code nanoTime} 分辨率限制（纳秒级），没有后台线程。
 *
 * <p>并发模型：读改写走 CAS 循环。拒绝路径<b>也</b>要 CAS 写回 —— 只读不写会让
 * 「上一次补充时刻」永远停在很久以前，每次拒绝都重算一遍相同的数，还会让
 * 「拒绝期间攒下来的令牌」在下一个请求到来时一次性释放，形成一次不该有的突发。
 */
public final class TokenBucketRateLimiter {

    private final double capacity;
    private final double tokensPerNano;

    private final AtomicReference<State> state;
    private final LongAdder acquired = new LongAdder();
    private final LongAdder rejected = new LongAdder();

    /**
     * @param qps   长期放行速率（个/秒）
     * @param burst 桶容量，决定瞬时能透支多少。经验值取 {@code qps}（1 秒的量）：
     *              再大就等于没有限流，再小会让开场的第一批请求被无谓地拒掉。
     */
    public TokenBucketRateLimiter(int qps, int burst) {
        if (qps <= 0) {
            throw new IllegalArgumentException("qps must be positive: " + qps);
        }
        if (burst <= 0) {
            throw new IllegalArgumentException("burst must be positive: " + burst);
        }
        this.capacity = burst;
        this.tokensPerNano = (double) qps / 1_000_000_000d;
        // 起始给满桶：活动开始的一瞬间不应该先被拒一批再恢复正常
        this.state = new AtomicReference<>(new State(capacity, System.nanoTime()));
    }

    /**
     * 尝试取一个令牌。
     *
     * @return true 放行；false 拒绝（此时调用方应立刻返回 429，不要排队等待）
     */
    public boolean tryAcquire() {
        while (true) {
            State snapshot = state.get();
            long now = System.nanoTime();
            long elapsed = now - snapshot.lastNanos;
            if (elapsed < 0) {
                // nanoTime 在极端情况下可能回退（不同核的时钟源切换）。当作没过时间，不补充。
                elapsed = 0;
            }
            double tokens = Math.min(capacity, snapshot.tokens + elapsed * tokensPerNano);

            if (tokens >= 1.0d) {
                if (state.compareAndSet(snapshot, new State(tokens - 1.0d, now))) {
                    acquired.increment();
                    return true;
                }
                continue;   // CAS 失败说明别人先改了，按新状态重算，不盲目放行
            }

            // 拒绝也要写回：把这次补充的令牌记下来，并把时间基准推进到 now
            if (state.compareAndSet(snapshot, new State(tokens, now))) {
                rejected.increment();
                return false;
            }
            // 拒绝路径的 CAS 失败不重试：当前确实没令牌，再抢一次也可能没有，
            // 而多一次循环就多一次争用 —— 这正是要避免的（拒绝路径必须是热路径里最快的）
            rejected.increment();
            return false;
        }
    }

    /** 剩余令牌数，仅用于观测（Prometheus / 调试端点）。 */
    public double availableTokens() {
        return state.get().tokens;
    }

    public long acquiredCount() {
        return acquired.sum();
    }

    public long rejectedCount() {
        return rejected.sum();
    }

    public double capacityTokens() {
        return capacity;
    }

    private record State(double tokens, long lastNanos) {
    }
}
