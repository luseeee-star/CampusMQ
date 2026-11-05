package com.campusslot.common.constant;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * 时间换算的唯一出口。
 *
 * <p>为什么值得单独一个类：这套系统里时间要经过三种表示——
 * <ul>
 *   <li>DB：{@code DATETIME}，不带时区，值本身必须已经是本地时间</li>
 *   <li>JVM：{@code LocalDateTime}，同样不带时区</li>
 *   <li>MQ / Redis：epoch 毫秒，是绝对时刻</li>
 * </ul>
 * 三种表示之间的转换如果散落在各处，只要有人用了 {@link ZoneId#systemDefault()}，
 * 换一台 TZ 不是 Asia/Shanghai 的机器（CI 容器默认 UTC）就会整体偏 8 小时——
 * 表现为「活动明明开放了，接口说过早」，或者「expire_at 全在 8 小时后」。
 * 这类 bug 在开发机上永远复现不了，因为开发机的 systemDefault 恰好是对的。
 *
 * <p>JDBC 侧已经用 {@code serverTimezone=Asia/Shanghai} 兜住了一半，
 * 但那只管 {@code java.sql.Timestamp}；{@code LocalDateTime} 是原样写入的，
 * 所以应用侧必须先算对。
 */
public final class AppTime {

    /** 与容器 TZ、JDBC serverTimezone、Jackson time-zone 三处保持一致。 */
    public static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");

    private AppTime() {
    }

    public static long toEpochMs(LocalDateTime time) {
        return time == null ? 0L : time.atZone(ZONE).toInstant().toEpochMilli();
    }

    public static LocalDateTime fromEpochMs(long epochMs) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZONE);
    }

    public static long nowMs() {
        return System.currentTimeMillis();
    }

    /**
     * 当前本地时间，<b>写库场景的唯一取法</b>。
     *
     * <p>存在的理由是 {@link LocalDateTime#now()} 用的是 {@link ZoneId#systemDefault()}，
     * 而本项目的 {@code DATETIME} 列一律按 {@link #ZONE} 存。开发机上两者恰好相同，
     * 所以这行代码永远看不出问题；换到 TZ=UTC 的 CI 或云主机上，写进去的值就比预期早 8 小时，
     * 而读出来又被当作本地时间解读——表现为「刚发的名额显示已超时」，
     * 或者「活动开放时间还没到」。这类差异不抛异常，只在换环境那天出现。
     *
     * <p>写成 {@code fromEpochMs(nowMs())} 而不是 {@code LocalDateTime.now(ZONE)}，
     * 是为了让「同一时刻的 epoch 表示」与「同一时刻的本地表示」必然一致：
     * 前者由 {@link #nowMs()} 定义，MQ 消息里的 {@code claimAtMs / expireAtMs} 也用它，
     * 三条路径共用同一个时刻来源。
     */
    public static LocalDateTime now() {
        return fromEpochMs(nowMs());
    }
}
