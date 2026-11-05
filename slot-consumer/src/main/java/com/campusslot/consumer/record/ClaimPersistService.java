package com.campusslot.consumer.record;

import com.campusslot.common.constant.AppTime;
import com.campusslot.common.entity.QuotaRecord;
import com.campusslot.common.mapper.QuotaCollegeMapper;
import com.campusslot.common.mapper.QuotaRecordMapper;
import com.campusslot.common.mapper.QuotaResourceMapper;
import com.campusslot.common.mq.ClaimMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 把一条 {@link ClaimMessage} 落进 MySQL。这是异步链路上唯一写库的地方。
 *
 * <p><b>整个 D3 的正确性都压在这 20 行里。</b>两个动作必须同生共死：
 * <pre>
 *   INSERT quota_record          →  成功：这条消息第一次被处理
 *                                抛 DuplicateKeyException：已经处理过（重投），必须停止，
 *                                绝不能重复扣 remain
 *   UPDATE resource SET remain-1 →  受影响行数 1：扣到了
 *                                受影响行数 0：DB 余量已经为 0，说明 Redis 与 DB 漂移了
 * </pre>
 *
 * <p><b>为什么 INSERT 不用 {@code INSERT IGNORE} / {@code ON DUPLICATE KEY UPDATE}</b>
 * （2026-10-27 改）：这两种写法都<em>不区分</em>「唯一键冲突」和「其它约束失败」。
 * {@code IGNORE} 把 CHECK 失败、外键失败一律降级成「受影响行数 0」，
 * 于是被当成「重投」ACK 掉，<b>消息永久消失，日志里只有一条 warn</b>。
 * {@code ON DUPLICATE KEY UPDATE} 更隐蔽：Connector/J 默认返回 found rows，
 * 冲突时返回 1，会被直接判成插入成功。
 * 现在用普通 INSERT + 异常分流，让「重复」成为一个明确的类型，而不是一个数字。
 *
 * <p><b>为什么"重复投递"绝对不能回补 Redis</b>（这一条最容易写反）：
 * 表里已经有这个学生时，有两种完全不同的成因，而<b>光看数据库分不出来</b>：
 * <ol>
 *   <li><b>消息重投</b>（RocketMQ 至少一次语义、消费者重启、ACK 丢失）——
 *       上一次投递已经 INSERT 成功并扣了 remain，Redis 也只扣过一次。
 *       此时回补 = 凭空多出一个名额 = <b>超卖</b>。</li>
 *   <li><b>学生重新抢</b>（幂等键过期后 Lua 当成全新请求又扣了一次 Redis）——
 *       此时 DB 没扣过，不回补 Redis 就少发一个。</li>
 * </ol>
 * 第一种天天发生，第二种几乎不可能（幂等键活到活动结束 + 1 天，活动关了也抢不了）。
 * 所以选择<b>不回补</b>：选错了只会少发一个，对账（D9）能查出来并补回去；
 * 选反了就是超发，而 R-1 的红线是"超发恒 0"。
 * <b>两种错代价不对等的时候，永远选代价小的那一边。</b>
 *
 * <p><b>为什么 DB 拒绝时也不回补</b>：{@code remain > 0} 不成立说明 Redis 比 DB 多算了一个名额，
 * 这条消息无论重试多少次都不会成功。回补 Redis 等于承认"这次没发生"，账确实会平，
 * 但下一次还会有人再撞一次同样的墙——问题被藏起来了。
 * 正确做法是 ACK 掉、打 P0、让对账把 {@code pending} 长期为正这件事暴露出来。
 */
@Service
public class ClaimPersistService {

    private static final Logger log = LoggerFactory.getLogger(ClaimPersistService.class);

    /** 落库结果。调用方据此决定 ACK 还是让 MQ 重试。 */
    public enum Status {
        /** 落库成功。 */
        PERSISTED,
        /** 唯一索引拦下：要么是消息重投，要么是幂等键过期后的重复申领。 */
        DUPLICATE,
        /** DB 侧余量已为 0，Redis 与 DB 漂移。P0。 */
        REMAIN_EXHAUSTED,
        /** DB 侧学院配额已为 0。P0。 */
        COLLEGE_EXHAUSTED,
        /** 数据库暂时不可用（连接、锁等待、死锁），应当重试。 */
        TRANSIENT,
        /**
         * 数据本身有硬缺陷（字段超长、CHECK 失败、NOT NULL 违规、外键不存在）——
         * 重试一万次也是同一个结果，必须 ACK 掉。
         */
        POISON
    }

    public record Result(Status status, String detail) {
        static Result of(Status status) {
            return new Result(status, null);
        }
    }

    private final QuotaRecordMapper recordMapper;    private final QuotaResourceMapper resourceMapper;
    private final QuotaCollegeMapper collegeMapper;
    private final TransactionTemplate tx;

    public ClaimPersistService(QuotaRecordMapper recordMapper,
                               QuotaResourceMapper resourceMapper,
                               QuotaCollegeMapper collegeMapper,
                               PlatformTransactionManager txManager) {
        this.recordMapper = recordMapper;
        this.resourceMapper = resourceMapper;
        this.collegeMapper = collegeMapper;
        this.tx = new TransactionTemplate(txManager);
    }

    public Result persist(ClaimMessage message) {
        AtomicReference<Status> rejected = new AtomicReference<>();
        LocalDateTime claimAt = AppTime.fromEpochMs(message.claimAtMs());
        LocalDateTime expireAt = AppTime.fromEpochMs(message.expireAtMs());

        try {
            Boolean ok = tx.execute(status -> {
                QuotaRecord record = new QuotaRecord();
                record.setResourceId(message.resourceId());
                record.setDeptId(message.deptId());
                record.setStudentId(message.studentId());
                record.setStatus("LOCKED");
                record.setSource(message.source() == null ? ClaimMessage.SOURCE_FCFS : message.source());
                record.setClaimAt(claimAt);
                record.setExpireAt(expireAt);
                // created_at / updated_at 也走应用时钟：这两列与 claim_at / expire_at 同在一行，
                // 若一列来自应用、一列来自服务器 NOW()，换时区后同一行的时间会互相矛盾。
                LocalDateTime writtenAt = AppTime.now();
                record.setCreatedAt(writtenAt);
                record.setUpdatedAt(writtenAt);

                // 先 INSERT 后 UPDATE：唯一索引是最便宜的去重，重复学生在这里就被挡住，
                // 不会白占一次行锁。顺序反过来的话，重复消息也会先扣 remain 再回滚，
                // 高并发下就是一片无谓的死锁重试。
                //
                // 这里曾经是 `if (insertIgnore(record) == 0)`。改成普通 INSERT 之后，
                // 「重复」不再靠返回值识别，而是靠 DuplicateKeyException —— 见外层 catch。
                // 返回值恒为 1，不做判断；万一为 0（SQL 被改坏），下面的 deductRemain
                // 会多扣一次，靠 quotaInvariant 判据能立刻发现。
                recordMapper.insertRecord(record);
                if (resourceMapper.deductRemain(message.resourceId(), writtenAt) == 0) {
                    rejected.set(Status.REMAIN_EXHAUSTED);
                    status.setRollbackOnly();
                    return Boolean.FALSE;
                }
                // 学院配额是第二层：总名额还有、但这个学院满了，同样必须拒绝。
                // 只在消息带了学院时才判 —— 没开配额的活动传的是 null，
                // 去 UPDATE 一个不存在的行会返回 0，把它当成"满"就会把所有申领拒掉。
                if (message.collegeCode() != null
                        && collegeMapper.deductRemain(message.resourceId(), message.collegeCode(), writtenAt) == 0) {
                    rejected.set(Status.COLLEGE_EXHAUSTED);
                    status.setRollbackOnly();
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;
            });

            if (Boolean.TRUE.equals(ok)) {
                return Result.of(Status.PERSISTED);
            }
            Status status = rejected.get();
            return status == null ? Result.of(Status.TRANSIENT) : Result.of(status);
        } catch (DuplicateKeyException e) {
            // 唯一索引 uk_record_resource_student 拦下 —— 真正的「重复」。
            // 这是唯一一条能安全 ACK 且不回补的路径，成因见类注释（重投 vs 重抢，一律不回补）。
            return new Result(Status.DUPLICATE, e.getMessage());
        } catch (DataAccessException e) {
            // 除了唯一键冲突以外的所有数据库错误。这里必须再分一次流，
            // 2026-10-28 D5 实测踩到了：一条 student_id 超长的消息被判成「可重试」，
            // 于是它被 Broker 以退避方式重投 16 次，每次约 1 分钟。
            // 后果不是多发或少发，而是<b>每一轮压测的 drain 都卡在它身上</b>：
            // pending 永远差 1，300 秒超时，整个实验跑不完。
            //
            // 判据不是「异常类型是不是 DataIntegrityViolationException」——那只能盖住一部分。
            // MySQL 的 CHECK 违反（3819）SQLState 是 HY000，Spring 只能翻成
            // UncategorizedSQLException，类型检查会漏掉它。所以按 <b>vendor code</b> 判：
            //   1406 字段超长 / 1265 截断 / 1366 非法字符 / 1292 类型转换
            //   1048 列不能为 NULL / 1452 外键不存在 / 3819 CHECK 违反
            // 这七种都在「同样的输入必然得同样的错」这一侧，重试没有任何信息增益。
            //
            // 为什么不干脆全部重试（原来就是这么写的）：可重试的那一类（连接、锁、死锁）
            // 会在几秒内自愈，重试确实有意义；而上面这七种是数据缺陷，重试只会
            // 让队列后面所有正常消息陪着一起等，把局部毛病放大成全局停顿。
            if (isPermanentDataDefect(e)) {
                log.error("P0 消息数据缺陷，重试无意义，已 ACK resourceId={} studentId={} reason={}",
                        message.resourceId(), message.studentId(),
                        e.getMostSpecificCause().getMessage());
                return new Result(Status.POISON, e.getMostSpecificCause().getMessage());
            }
            // 剩下的才是真的「暂时」：连接池耗尽、死锁、锁等待超时。
            // 退避由 Broker 按重试次数控制，不在这里 sleep —— sleep 会把同队列后面的消息一起拖住。
            log.warn("落库失败，交给 MQ 重试 resourceId={} studentId={} reason={}",
                    message.resourceId(), message.studentId(), e.getMostSpecificCause().getMessage());
            return new Result(Status.TRANSIENT, e.getMostSpecificCause().getMessage());
        } catch (RuntimeException e) {
            // 提交阶段失败（TransactionSystemException）、事务超时等不属于 DataAccessException 的
            // 运行时异常。漏掉这一层的话异常会穿透到 MQ 监听器，消费线程状态不可预期。
            // 一律按「可能成功、可能失败」处理，返回 TRANSIENT 让 MQ 重投 ——
            // 重投时唯一索引会把「其实已经成功」的那条挡成 DUPLICATE，不会重复扣。
            log.warn("落库异常（非 DAO 异常），按可重试处理 resourceId={} studentId={}",
                    message.resourceId(), message.studentId(), e);
            return new Result(Status.TRANSIENT, e.getMessage());
        }
    }

    /**
     * 这个数据库错误是不是「同样的输入必然得同样的错」。
     *
     * <p>只看异常类型不够：Spring 对 MySQL 的 CHECK 违反（3819）只能翻成
     * {@code UncategorizedSQLException}（SQLState 是 HY000，不是 23xxx），
     * 而字段超长（1406）走的是 {@code DataIntegrityViolationException}。两类都是永久的，
     * 却落在不同的类型上。所以往下挖到 {@link SQLException} 读 vendor code。
     *
     * <p>vendor code 硬编码是刻意的：这些编号是 MySQL 协议的一部分，二十年没变过，
     * 比匹配 "Data too long" 这种可被 localize 的英文消息稳。代价是换库（MariaDB 基本一致、
     * PostgreSQL 不同）时要维护这张表——放在这里比散在 catch 块里好找。
     */
    private static boolean isPermanentDataDefect(DataAccessException e) {
        Throwable cause = e.getMostSpecificCause();
        if (cause instanceof SQLException sql) {
            return switch (sql.getErrorCode()) {
                case 1406, 1265, 1366, 1292, 1048, 1452, 3819 -> true;
                default -> false;
            };
        }
        return false;
    }
}
