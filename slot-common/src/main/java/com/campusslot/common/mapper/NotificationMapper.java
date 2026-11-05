package com.campusslot.common.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.campusslot.common.entity.Notification;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * 通知记录。`(biz_id, channel)` 唯一索引 + INSERT IGNORE 保证重复消费不产生重复通知（SYS-2）。
 *
 * <p><b>两条 SQL 的时钟都改成参数了。</b>{@code created_at} 与 {@code next_retry_at}
 * 都是应用侧按 {@code AppTime.ZONE} 算出来的时刻，而 {@code NOW()} 取的是 MySQL 服务器时区。
 * 一旦两者不等（云库默认 UTC），{@code next_retry_at <= NOW()} 会整体偏移 8 小时：
 * 要么所有通知立刻"到期"被连续重试，要么一条都不重试且日志里没有任何异常。
 * 这与 {@link QuotaRecordMapper} 的处理保持同一口径：<b>DB 侧不出现 {@code NOW()}</b>。
 *
 * <p><b>为什么不加 {@code record_id} 列。</b>{@code quota_record} 上有
 * {@code uk_record_resource_student (resource_id, student_id)}，所以 {@code (resource_id, student_id)}
 * 本身就是与 recordId 一一对应的唯一键、且带索引——D9 要按 {@code release_reason} 分类时，
 * 用 {@code JOIN quota_record USING (resource_id, student_id)} 是单行精确命中，不是"软关联"。
 * 加一列要在四个写点分别填值（其中确认路径刻意不回查 recordId），换来的只是一份冗余；
 * 而且一旦哪天允许"释放后再抢"（那本身就要改上面的唯一键），这列会变成过期副本。
 * 真要加，应该和"放开唯一键"一起加。
 */
@Mapper
public interface NotificationMapper extends BaseMapper<Notification> {

    /**
     * <b>调用方必须设置 {@code createdAt = AppTime.now()}</b>。列是 {@code NOT NULL}，
     * 显式传 NULL 不会回落到 {@code DEFAULT CURRENT_TIMESTAMP}，而是直接报
     * "Column 'created_at' cannot be null"——这是刻意的：让它快速失败，
     * 而不是让 DB 时钟悄悄填一个不同口径的值进来。
     *
     * <p><b>不写 {@code next_retry_at}</b>，所以新行是 NULL，含义是"从未尝试过"。
     * {@link #selectRetryable} 里 {@code next_retry_at IS NULL} 分支就是为它准备的：
     * 新通知必须被立刻捞出，不能等到某个时刻。
     *
     * <p>{@code IGNORE} 吞掉的不只是重复键，还有截断与 NOT NULL 违例（都降级成 warning）。
     * 这里的字段全部由代码拼装、长度可控，风险可接受；但不要因此把它当成"写通知不会失败"。
     */
    @Insert("""
            INSERT IGNORE INTO notification
                (biz_id, student_id, resource_id, channel, title, content, status, retry_count, created_at)
            VALUES
                (#{bizId}, #{studentId}, #{resourceId}, #{channel}, #{title}, #{content}, #{status}, 0, #{createdAt})
            """)
    int insertIgnore(Notification notification);

    /**
     * 取出到期待重试的通知。
     * 必须带上 next_retry_at 与 retry_count 上限：只按 status='FAILED' 扫，
     * 一条永久失败的通知会被无限重试，最后把通知通道打死。
     *
     * <p><b>三个条件缺一不可，{@code status IN ('PENDING','FAILED')} 与
     * {@code next_retry_at IS NULL} 都不是可选项</b>：
     * <ul>
     *   <li>只写 {@code status='PENDING'} → 首次发送失败后变 FAILED，从此再也捞不到，
     *       表现为"通知发了一次就消失"，日志里什么都没有；</li>
     *   <li>漏掉 {@code IS NULL} → 新写入的行（{@code insertIgnore} 不填 next_retry_at）
     *       永远不满足 {@code next_retry_at <= now}，首次发送<b>永远不触发</b>。</li>
     * </ul>
     *
     * <p><b>{@code retry_count} 是"已尝试次数（含首次）"</b>，所以 {@code retry_count < maxRetry}
     * 的含义是"还有尝试机会"。它与 {@code markDead} 的"第 {@code maxRetry} 次失败同轮置 DEAD"
     * 是配对的：少了后者，{@code retry_count = maxRetry} 的行既捞不出来、也不标死，成为幽灵行。
     */
    @Select("""
            SELECT * FROM notification
             WHERE status IN ('PENDING', 'FAILED')
               AND retry_count < #{maxRetry}
               AND (next_retry_at IS NULL OR next_retry_at <= #{now})
             ORDER BY id
             LIMIT #{limit}
            """)
    List<Notification> selectRetryable(@Param("now") LocalDateTime now,
                                       @Param("maxRetry") int maxRetry,
                                       @Param("limit") int limit);

    /**
     * 发送成功。
     *
     * <p>带 {@code status IN ('PENDING','FAILED')} 是为了多实例：两台同时扫到同一条时，
     * 只有一台的 UPDATE 命中；另一台即使已经把消息发出去，也改不动状态。
     * 这把"重复发送"收窄成"极小概率重复 + 状态不被覆盖"，与扫表的条件更新同一套纪律。
     */
    @Update("""
            UPDATE notification
               SET status = 'SENT', sent_at = #{sentAt}, retry_count = #{retryCount}, error_msg = NULL
             WHERE id = #{id} AND status IN ('PENDING', 'FAILED')
            """)
    int markSent(@Param("id") long id,
                 @Param("retryCount") int retryCount,
                 @Param("sentAt") LocalDateTime sentAt);

    /** 发送失败、还有重试额度：记次数 + 排下次时刻（指数退避）。 */
    @Update("""
            UPDATE notification
               SET status = 'FAILED', retry_count = #{retryCount},
                   next_retry_at = #{nextRetryAt}, error_msg = #{errorMsg}
             WHERE id = #{id} AND status IN ('PENDING', 'FAILED')
            """)
    int markRetry(@Param("id") long id,
                  @Param("retryCount") int retryCount,
                  @Param("nextRetryAt") LocalDateTime nextRetryAt,
                  @Param("errorMsg") String errorMsg);

    /**
     * 额度用尽，置 DEAD。
     *
     * <p><b>必须在"最后一次失败"的同一轮里置，不能等下一轮扫描再判。</b>
     * {@code selectRetryable} 的条件是 {@code retry_count < maxRetry}，
     * 一条 {@code retry_count = maxRetry} 的 FAILED 行从此既不会被捞出、也不会进 DEAD，
     * 报表里就是一条永远悬着的幽灵行。
     *
     * <p>DEAD 的语义是"停止自动重试、留给人工重发"，不是"放弃"：
     * 真实通道抖一分钟就会有一批进 DEAD，把它们标成"永久失败"会让人误判成代码缺陷。
     */
    @Update("""
            UPDATE notification
               SET status = 'DEAD', retry_count = #{retryCount},
                   next_retry_at = NULL, error_msg = #{errorMsg}
             WHERE id = #{id} AND status IN ('PENDING', 'FAILED')
            """)
    int markDead(@Param("id") long id,
                 @Param("retryCount") int retryCount,
                 @Param("errorMsg") String errorMsg);

    /** 按状态计数，给 dev 判据与 D9 的送达率报表用。 */
    @Select("""
            SELECT status, COUNT(*) AS cnt
              FROM notification
             WHERE resource_id = #{resourceId}
             GROUP BY status
            """)
    List<Map<String, Object>> countByStatus(@Param("resourceId") long resourceId);
}
