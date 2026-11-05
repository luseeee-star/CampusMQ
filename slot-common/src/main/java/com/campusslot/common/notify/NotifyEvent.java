package com.campusslot.common.notify;

/**
 * 通知事件与它的业务键（SYS-2）。
 *
 * <p><b>业务键前缀只能由「事件」决定，不能由「入口」决定。</b>
 * {@code ReleaseService.release(...)} 被 S-4（学生取消）和 SYS-1（超时释放）共用，
 * 如果前缀在入口处写死，将来加第三个 reason 就会静默写错前缀——而 {@code (biz_id, channel)}
 * 是唯一键，写错前缀不会报错，只会多出一条永远不会被合并的重复通知。
 * 所以前缀放在枚举里：调用方只传「这件事是什么」，不传「字符串怎么拼」。
 *
 * <p><b>业务键形如 {@code confirm:3:S2024001}</b>：{@code 事件:资源:学号}。
 * 不用 {@code recordId} 的原因见 {@code NotificationMapper} 的类注释——确认路径
 * 刻意不回传 recordId，而 {@code uk_record_resource_student} 保证 (resourceId, studentId)
 * 与 recordId 一一对应，两者等价但前者在所有入口都拿得到。
 *
 * <p>同一学生对同一资源再次触发同类事件（重复点击、消息重投、取消后又被扫到）时，
 * 唯一键让第二次写入变成 no-op，不会产生第二条通知——这正是 {@code INSERT IGNORE} 的用途。
 */
public enum NotifyEvent {

    /** 锁定成功（写点在 {@code cg_notify} 消费者，不在申领主链路）。 */
    LOCK("名额已锁定", "你已锁定名额，请在确认时限内完成确认"),

    /** 确认成功（S-3）。 */
    CONFIRM("确认成功", "名额已确认，请按时参加"),

    /** 学生主动取消（S-4）。 */
    CANCEL("已取消", "你已取消本次名额，名额已释放给其他人"),

    /** 超时释放（SYS-1）。 */
    RELEASE("名额已释放", "超过确认时限，名额已自动释放");

    private final String title;
    private final String content;

    NotifyEvent(String title, String content) {
        this.title = title;
        this.content = content;
    }

    public String title() {
        return title;
    }

    public String content() {
        return content;
    }

    /**
     * 业务唯一键。前缀取枚举常量的小写形式：{@code lock / confirm / cancel / release}。
     *
     * <p>长度上限是 {@code biz_id VARCHAR(128)}：{@code release}(7) + 1 + 资源 id + 1 + 学号(≤32)，
     * 资源 id 是 BIGINT，最坏 19 位，合计 60 以内，留了足够余量。
     */
    public String bizId(long resourceId, String studentId) {
        return name().toLowerCase() + ":" + resourceId + ":" + studentId;
    }
}
