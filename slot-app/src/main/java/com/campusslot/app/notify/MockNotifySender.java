package com.campusslot.app.notify;

import com.campusslot.common.entity.Notification;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 通知通道的替身（SYS-2 明确不接真实短信/企微）。
 *
 * <p><b>失败必须按"尝试次数"确定性注入，不能按概率。</b>
 * 若用 {@code failRate=0.5}，一条消息 3 次全失败的概率是 12.5%——
 * 50 条里至少一条进 DEAD 的概率接近 100%，验收会变成"一半概率红"，
 * 而人看到红灯的第一反应是去查代码，不是去查概率。
 * 这里改成"前 k 次尝试一律失败"：{@code k=0} 全成功（默认）、{@code k=1} 每条失败一次后成功、
 * {@code k>=maxRetry} 全部进 DEAD。三种状态都能稳定复现。
 *
 * <p>运行时可通过 {@code POST /api/claim/dev/notify-mock?failAttempts=k} 改，
 * 不用重启——验收要在一个 JVM 里连续跑三种注入，重启会把"冷启动慢"混进来。
 */
@Component
public class MockNotifySender {

    /** 前 N 次尝试强制失败（N=0 表示从不失败）。 */
    private final AtomicInteger failAttempts;

    public MockNotifySender(@Value("${campusslot.notify.mock-fail-attempts:0}") int failAttempts) {
        this.failAttempts = new AtomicInteger(Math.max(0, failAttempts));
    }

    /**
     * @param attempt 本次是第几次尝试（1 起）。调用方用 {@code retry_count + 1} 算出来，
     *                所以注入值只跟"这条通知尝试了几次"有关，与重启、与并发无关。
     * @return true 表示"发送成功"
     */
    public boolean send(Notification notification, int attempt) {
        return attempt > failAttempts.get();
    }

    public int failAttempts() {
        return failAttempts.get();
    }

    public void setFailAttempts(int value) {
        failAttempts.set(Math.max(0, value));
    }
}
