package com.campusslot.common.mq;

/**
 * {@code quota_claimed} 的消息体：一次「Redis 已扣、DB 还没写」的申领。
 *
 * <p>它是整条链路上唯一跨越两个进程的数据结构，所以刻意做成扁平 record +
 * 纯 JSON，不用 Java 序列化：换语言、换客户端版本、用 Dashboard 手工重投一条消息，
 * 都不需要两边同时升级。
 *
 * <p><b>为什么把 {@code claimAtMs} / {@code expireAtMs} 定在发送端算</b>：
 * 消费端算的话，重试投递会算出不同的截止时刻，同一条记录在两次投递里 expire_at 不一样，
 * 唯一的区别只在日志里能看到，线上表现为「同一个人有时 30 分钟释放、有时 31 分钟」。
 * 时间只有一个来源。
 *
 * <p><b>{@code collegeCode} 为空 vs 非空</b>：
 * 空表示这场活动没开学院配额，消费端不得去动 {@code quota_college}。
 * 用空值而不是 "NONE" 之类的哨兵字符串，是因为 null 在 JSON 里天然缺席，
 * 少一个「判断字符串内容」的分支，也就少一个写错的机会。
 *
 * @param resourceId  资源 ID
 * @param deptId      部门 ID，落库时直接写进 quota_record（避免消费端再查一次资源）
 * @param studentId   学号
 * @param collegeCode 学院编码；未启用学院配额时为 null
 * @param claimAtMs   申领时刻（epoch 毫秒），= 首次扣减成功的时刻
 * @param expireAtMs  确认截止时刻，= claimAt + confirm_timeout_sec
 * @param source      FCFS / BACKFILL
 */
public record ClaimMessage(long resourceId,
                           long deptId,
                           String studentId,
                           String collegeCode,
                           long claimAtMs,
                           long expireAtMs,
                           String source) {

    public static final String SOURCE_FCFS = "FCFS";
    public static final String SOURCE_BACKFILL = "BACKFILL";

    public ClaimMessage {
        if (resourceId <= 0) {
            throw new IllegalArgumentException("resourceId must be positive: " + resourceId);
        }
        if (studentId == null || studentId.isBlank()) {
            throw new IllegalArgumentException("studentId must not be blank");
        }
    }

    /**
     * MQ 的 message key。
     *
     * <p>用 {@code resourceId:studentId} 而不是随机 UUID：RocketMQ 的 key 是索引，
     * 出问题时能按 `resourceId` 或 `resourceId:学号` 在 Dashboard 上直接把这条消息捞出来，
     * 不需要去日志里对着时间戳猜。
     */
    public String messageKey() {
        return resourceId + ":" + studentId;
    }
}
