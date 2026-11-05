package com.campusslot.app.controller;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.LongAdder;

/**
 * 空转端点，唯一用途：<b>标定压测器的天花板</b>。
 *
 * <p><b>为什么必须有这个东西</b>：D5 要找「单 key 的 Redis 拐点」，这条曲线是报告的核心数字之一。
 * 但如果 {@code BaselineHammer} 自己在 1200 req/s 就饱和了，那么测到的「拐点」是压测器的，
 * 不是 Redis 的——而且<b>两者长得一模一样</b>：吞吐不再上升。光看曲线分不出来。
 *
 * <p>所以先把压测器自己的上限量出来：用同一个 hammer、同样参数打这个端点，
 * 链路上只有 Tomcat + Spring MVC + HttpMessageConverter，不碰 DB、不碰 Redis、不碰 MQ。
 * 判据是 <b>Redis 拐点必须明显低于这条线的峰值</b>，否则 D5 那条曲线要打问号。
 *
 * <p><b>这个上限不是常数，是一条曲线。</b>hammer 用的是平台线程，
 * 200 并发就是 200 个 OS 线程，上下文切换开销随并发上升，吞吐可能在某个点<b>反着往下走</b>。
 * 所以标定方式是扫 concurrency（50/100/200/400/800）画一条曲线，而不是打一炮看峰值。
 *
 * <p><b>为什么返回值是 {@code "pong"} 而不是 JSON</b>：JSON 序列化本身是 CPU 开销，
 * 会把测出来的上限压低一截，而那截开销和 Redis 无关。返回 4 字节纯文本，
 * 让这个数字尽量只反映「HTTP 栈 + 线程调度」的成本。真实接口（含 JSON）的上限只会更低，
 * 不会更高——这个方向是安全的。
 *
 * <p>带一个 {@link LongAdder} 计数：压完之后对比「hammer 记录的成功数」与这个数，
 * 差得多说明客户端侧有请求根本没发出去（连接被拒、超时未计入），那种情况下吞吐数字也是假的。
 * 用 {@code LongAdder} 而不是 {@code AtomicLong}：后者在高并发下是单个 CAS 热点，
 * 会人为制造一个和业务无关的瓶颈，把这个端点自己的上限拉到比真实值更低。
 */
@RestController
@RequestMapping("/api/dev")
@ConditionalOnProperty(name = "campusslot.dev-endpoints", havingValue = "true")
public class PingController {

    private final LongAdder hits = new LongAdder();

    @GetMapping("/ping")
    public String ping() {
        hits.increment();
        return "pong";
    }

    /**
     * 服务端实际收到的请求数。压完读一次，与 hammer 报的成功数比对。
     *
     * <p>这个数比 hammer 自己的统计更可信：hammer 的计数在客户端，
     * 连接被重置、请求在发出前就失败，它都可能看不见。
     */
    @GetMapping("/ping/count")
    public String count() {
        return String.valueOf(hits.sum());
    }

    /** 换轮次前清零，否则累加值会让下一轮的比对失真。 */
    @GetMapping("/ping/reset")
    public String reset() {
        hits.reset();
        return "ok";
    }
}
