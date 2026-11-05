package com.campusslot.common.constant;

/**
 * Topic / 消费组规划。队列数在 deploy/topic-init.sh 里显式指定，不依赖自动创建。
 *
 * <p>v2.0 只保留三个 Topic，与需求书 §5.3 的三类系统任务一一对应：
 * <ul>
 *   <li>{@code quota_claimed} —— §4.2 步骤 H 的「异步落库」，同时供通知消费者订阅（不同消费组各自收全量）</li>
 *   <li>{@code quota_release_delay} —— SYS-1「锁定满 30 分钟未确认，到点触发」，用 5.x 任意时间定时消息。
 *       <b>目前只有定义，没有生产发送点也没有业务消费端</b>（2026-11-04），SYS-1 实际靠扫表兜底</li>
 *   <li>{@code quota_backfill} —— R-6「释放的名额进补位队列定向通知」</li>
 * </ul>
 *
 * <p>v2.0 删除了 {@code quota_lottery_entry}（不做抽签）与 {@code quota_reconcile}：
 * §5.3 的 SYS-3 对账巡检是<b>每 5 分钟的定时任务</b>，不是事件流。
 * 把它做成 MQ 流，会得到一条「对账消息本身也要被对账」的链路，纯属自找麻烦。
 */
public final class MqTopic {

    private MqTopic() {
    }

    /** 申领主业务事件：学生已锁定名额。消费组：cg_record（落库）/ cg_notify（锁定通知）。 */
    public static final String QUOTA_CLAIMED = "quota_claimed";
    public static final int QUOTA_CLAIMED_QUEUES = 16;

    /**
     * {@code quota_claimed} 的 tag：申领成功事件。
     *
     * <p>现在只有一个 tag，看上去像是多余的。留着它的原因是 D8 之后同一个 Topic 上
     * 还会出现「补位成功」「释放」这类同族事件，那时用 tag 过滤比给每个事件单开 Topic 便宜得多
     * （Topic 建一个就要维护一套队列与位点）。
     * 消费者的订阅表达式现在是 SUB_ALL，将来按 tag 收窄只需要改一行。
     */
    public static final String TAG_CLAIM = "CLAIM";

    /**
     * 30 分钟未确认释放的定时消息（SYS-1）。
     * 队列数给 8：释放是到点集中的，队列太少会让一批同时到期的消息排队消费，
     * 表现为「释放误差 P99 突然变差」，而 NFR-5 要求 &lt; 1s。
     *
     * <p>目前未接入：没有发送点，也没有业务消费端（{@code DevSmokeConsumer} 只是订阅做连通性烟测）。
     */
    public static final String QUOTA_RELEASE_DELAY = "quota_release_delay";
    public static final int QUOTA_RELEASE_DELAY_QUEUES = 8;

    /** 补位定向通知（R-6），量小，4 个队列足够。 */
    public static final String QUOTA_BACKFILL = "quota_backfill";
    public static final int QUOTA_BACKFILL_QUEUES = 4;

    /** 落库：唯一在同步链路之外写 MySQL 的地方。 */
    public static final String CG_RECORD = "cg_record";

    /** 通知投递：与 cg_record 并列订阅同一 Topic，停掉它不影响落库（故障注入用）。 */
    public static final String CG_NOTIFY = "cg_notify";

    /** 超时释放。 */
    public static final String CG_RELEASE = "cg_release";

    /** 补位定向通知。 */
    public static final String CG_BACKFILL = "cg_backfill";

    /**
     * dev 冒烟组：只收 {@code smoke} / {@code delay} 两个 tag，用于 D1 的三个验证点
     * （定时消息精度、队列数、链路连通）。
     *
     * <p>它必须和 {@link #CG_RECORD} 分开。D3 之前两者合在一个组里，
     * {@code /api/dev/mq/send} 发的手工消息会被落库消费者一起收走；
     * 一旦消费者真的开始写 {@code quota_record}，这就变成"每调试一次就往库里塞垃圾"。
     * 消费组的第一个设计原则是<b>「谁的写入，谁定过滤条件」</b>。
     */
    public static final String CG_SMOKE = "cg_smoke";
}
