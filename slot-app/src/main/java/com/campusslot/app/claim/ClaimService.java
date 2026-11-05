package com.campusslot.app.claim;

import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.RedisKeys;
import com.campusslot.common.constant.StudentIdRule;
import com.campusslot.common.entity.QuotaRecord;
import com.campusslot.common.entity.QuotaResource;
import com.campusslot.common.mapper.QuotaRecordMapper;
import com.campusslot.common.mapper.QuotaResourceMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 申领主链路（D2 版本：Redis Lua 原子扣减 + 同步落库）。
 *
 * <p>相对 D1 裸版的唯一改动是<b>把判定搬到了 Redis</b>：
 * <pre>
 *   D1  SELECT remain  →  代码里 if (remain > 0)  →  UPDATE remain-1
 *       ↑ 三步之间任何两个请求都能插进来，这是超卖的根
 *
 *   D2  EVAL claim_basic.lua（GET + 判断 + DECR 在一个脚本里）
 *       ↑ 单线程执行期间不会被打断，判定与扣减之间没有窗口
 * </pre>
 *
 * <p><b>本版仍然是同步落库</b>，DB 还留在请求路径上：一次 {@code selectById} 读资源元信息，
 * 加上事务里的 {@code INSERT} 与 {@code UPDATE}。所以 D2 的收益是<b>正确性</b>（超卖归零），
 * 不是吞吐——吞吐要等 D3 把落库搬进 MQ 才会明显变化。
 * 这一点在压测数据里会看得很清楚，不要在报告里把 D2 写成性能优化。
 *
 * <p>三层防线的分工（需求书 §8.2），D2 完成前两层：
 * <table border="1">
 *   <caption>防超卖的三层</caption>
 *   <tr><th>层</th><th>手段</th><th>作用</th></tr>
 *   <tr><td>第一层</td><td>Redis 原子扣减</td><td>挡住绝大部分并发，入口处就不放过去</td></tr>
 *   <tr><td>第二层</td><td>{@code UPDATE ... WHERE remain > 0} 看受影响行数</td><td>Redis 挂了或漂移时兜底，宁可少发不可超发</td></tr>
 *   <tr><td>第三层</td><td>{@code uk_record_resource_student} 唯一索引</td><td>一人一条，重复投递落不进来</td></tr>
 * </table>
 *
 * <p><b>D3 起主链路已经换到 {@link AsyncClaimService}</b>（Redis 判定 + MQ 异步落库 + R-3 首次结果
 * + G-1 学院配额）。本类保留下来有三个用途，都不是"忘了删"：
 * <ol>
 *   <li>对照组②的复现入口：{@code /api/claim} 永远能跑出 D2 那一组数据</li>
 *   <li>MQ 不可用时的降级参考实现：同步落库虽然慢，但它不依赖 Broker</li>
 *   <li>D7 分片时用它做"同步 vs 分片"的对照，避免一次改动混进两个变量</li>
 * </ol>
 *
 * <p>改这个类之前先想清楚：它现在的价值是<b>不变</b>。任何"顺手优化"
 * 都会让 D2 那组历史数据失去可比性。
 */
@Service
public class ClaimService {

    private static final Logger log = LoggerFactory.getLogger(ClaimService.class);

    /** 与 claim_basic.lua 的返回码一一对应，改脚本时必须同步改这里。 */
    private static final int CODE_OK = 1;
    private static final int CODE_FULL = 0;
    private static final int CODE_DUPLICATE = -1;
    private static final int CODE_STOCK_MISSING = -2;

    private final StringRedisTemplate redis;
    private final QuotaResourceMapper resourceMapper;
    private final QuotaRecordMapper recordMapper;
    private final RedisScript<List> claimBasicScript;
    private final TransactionTemplate tx;
    private final int idemTtlExtraSec;

    public ClaimService(StringRedisTemplate redis,
                        QuotaResourceMapper resourceMapper,
                        QuotaRecordMapper recordMapper,
                        @Qualifier("claimBasicScript") RedisScript<List> claimBasicScript,
                        PlatformTransactionManager txManager,
                        @Value("${campusslot.claim.idem-ttl-extra-sec:600}") int idemTtlExtraSec) {
        this.redis = redis;
        this.resourceMapper = resourceMapper;
        this.recordMapper = recordMapper;
        this.claimBasicScript = claimBasicScript;
        // 用编程式事务而不是 @Transactional：下面的 persist() 是本类内部调用，
        // 走代理的注解在这里不生效（自调用不经过 AOP），会被静默忽略成一个无事务的普通方法。
        // 这类 bug 不会报错，只会让「INSERT 成功但 UPDATE 失败」留下半条脏数据。
        this.tx = new TransactionTemplate(txManager);
        this.idemTtlExtraSec = idemTtlExtraSec;
    }

    public ClaimOutcome claim(long resourceId, String studentId) {
        // 与 /api/claim/mq 共用同一条规则：非法学号不能先扣掉一个 Redis 名额再被 DB 拒绝。
        // 两条链路各写一份 if 迟早会分叉，所以规则收在 StudentIdRule 里。
        if (!StudentIdRule.isValid(studentId)) {
            return ClaimOutcome.fail(ErrorCode.PARAM_INVALID, StudentIdRule.rejectReason(studentId));
        }

        // 每次请求读一次资源元信息。这是 D2 剩下的两次 DB 往返之一，
        // D3 把它和落库一起搬走（或者 D12 用本地缓存），在报告里要算进 RT 对比。
        QuotaResource res = resourceMapper.selectById(resourceId);
        if (res == null) {
            return ClaimOutcome.fail(ErrorCode.RESOURCE_NOT_FOUND);
        }
        if (!isOpen(res)) {
            return ClaimOutcome.fail(ErrorCode.RESOURCE_NOT_OPEN);
        }

        int confirmTimeoutSec = res.getConfirmTimeoutSec() == null ? 1800 : res.getConfirmTimeoutSec();
        long idemTtlMs = (confirmTimeoutSec + idemTtlExtraSec) * 1000L;

        int code = evalClaim(resourceId, studentId, idemTtlMs);
        switch (code) {
            case CODE_OK -> {
                // 扣减成功，继续落库
            }
            case CODE_FULL -> {
                return ClaimOutcome.fail(ErrorCode.QUOTA_FULL);
            }
            case CODE_DUPLICATE -> {
                return ClaimOutcome.fail(ErrorCode.ALREADY_CLAIMED);
            }
            case CODE_STOCK_MISSING -> {
                // 库存 key 没了，这是 P0。绝不能当「已满」返回：
                // 学生会看到"名额已满"然后放弃，而实际上是我们的缓存丢了，名额还在。
                log.error("P0 库存 key 缺失 resourceId={} studentId={}", resourceId, studentId);
                return ClaimOutcome.fail(ErrorCode.STOCK_CACHE_MISSING);
            }
            default -> {
                log.error("claim_basic.lua 返回未知码 {} resourceId={} studentId={}",
                        code, resourceId, studentId);
                return ClaimOutcome.fail(ErrorCode.SYSTEM_BUSY);
            }
        }

        PersistOutcome persisted = persist(res, studentId, confirmTimeoutSec);
        if (!persisted.ok()) {
            if ("DUPLICATE".equals(persisted.reason())) {
                // DB 里已经有这个学生的记录，只是 Redis 幂等键过期或丢了。
                // 本条请求没有真的占用名额，把刚扣的 1 个还回去；幂等键保留，后续重复点击继续短路。
                compensate(resourceId, studentId, false);
                return ClaimOutcome.fail(ErrorCode.ALREADY_CLAIMED);
            }
            if ("DB_ERROR".equals(persisted.reason())) {
                // 约束失败（CHECK / 外键 / 超长）：名额必须还回去 —— Redis 扣了，库里什么都没落。
                // 幂等键也一起删掉，否则学生这一轮会被判「已参与」，连重试的机会都没有。
                // 返回的是系统错误而不是 2006：让他刷新重试是对的，告诉他「重复申领」是错的。
                compensate(resourceId, studentId, true);
                return ClaimOutcome.fail(ErrorCode.SYSTEM_BUSY, "落库失败，请稍后重试");
            }
            // Redis 说还有余量，DB 说没有了 —— 两层数据已经漂移。
            // DB 是最后一道防线，宁可拒绝这次请求，也不能让它落库造成超发。
            log.error("P0 库存漂移：Redis 扣减成功但 DB remain 已为 0, resourceId={} studentId={}",
                    resourceId, studentId);
            compensate(resourceId, studentId, true);
            return ClaimOutcome.fail(ErrorCode.QUOTA_FULL, "库存状态异常，已阻断本次申领");
        }

        return ClaimOutcome.ok(0);
    }

    private boolean isOpen(QuotaResource res) {
        if (!"PUBLISHED".equals(res.getStatus())) {
            return false;
        }
        LocalDateTime now = AppTime.now();
        if (res.getOpenAt() != null && now.isBefore(res.getOpenAt())) {
            return false;
        }
        return res.getCloseAt() == null || !now.isAfter(res.getCloseAt());
    }

    /**
     * 执行 Lua。
     *
     * <p>返回值按 {@code List} 接而不是 {@code Long}：{@code StringRedisTemplate} 的
     * valueSerializer 是字符串序列化器，直接把脚本的整数返回值声明成 {@code Long}
     * 会在反序列化阶段炸掉。这是 v2.0 改版时踩过的坑，已记进 LOGBOOK。
     */
    private int evalClaim(long resourceId, String studentId, long idemTtlMs) {
        List<?> raw;
        try {
            raw = redis.execute(claimBasicScript,
                    List.of(RedisKeys.stock(resourceId), RedisKeys.idem(resourceId, studentId)),
                    String.valueOf(idemTtlMs));
        } catch (Exception e) {
            // Redis 挂了：不降级成"直接打 DB"，那会瞬间把 8 倍流量灌到只有 64 个连接的连接池上。
            // 快速失败，让网关限流（D6）和前端重试去处理。
            log.error("Redis 执行 claim_basic.lua 失败 resourceId={} studentId={}", resourceId, studentId, e);
            return CODE_STOCK_MISSING;
        }
        if (raw == null || raw.isEmpty() || raw.get(0) == null) {
            return CODE_STOCK_MISSING;
        }
        try {
            return Integer.parseInt(String.valueOf(raw.get(0)).trim());
        } catch (NumberFormatException e) {
            log.error("claim_basic.lua 返回值无法解析: {}", raw);
            return CODE_STOCK_MISSING;
        }
    }

    /**
     * 落库：INSERT 记录 + UPDATE 余量，两步必须在一个事务里。
     *
     * <p>顺序是「先 INSERT 后 UPDATE」：唯一索引是最便宜的去重手段，
     * 重复学生在这里就被挡住，不会白占一次行锁。
     *
     * <p><b>INSERT 不再用 {@code INSERT IGNORE}</b>（2026-10-27 与消费者侧同步改）：
     * {@code IGNORE} 会把 CHECK 失败、外键失败、字段超长一起降级成「受影响行数 0」，
     * 于是被当成 {@code DUPLICATE} —— 学生收到 2006「你已申领过」，
     * 而真相是我们自己的约束炸了，他重试多少次都还是 2006。
     * 现在重复靠 {@link DuplicateKeyException} 识别，其它数据库错误单列 {@code DB_ERROR}。
     *
     * <p>三个失败原因要分开返回，因为善后动作不同：
     * <ul>
     *   <li>{@code DUPLICATE} —— 学生已有记录，名额没被占用，回补余量但<b>保留</b>幂等键</li>
     *   <li>{@code DB_ERROR} —— 约束失败，回补余量并<b>删掉</b>幂等键，返回系统错误</li>
     *   <li>{@code DB_REMAIN_ZERO} —— Redis 与 DB 漂移，回补余量并删掉幂等键，还要告警</li>
     * </ul>
     */
    private PersistOutcome persist(QuotaResource res, String studentId, int confirmTimeoutSec) {
        // AppTime.now() 而不是 LocalDateTime.now()：后者取 JVM 默认时区，写进 DATETIME 列后
        // 与 MQ 侧（AppTime.fromEpochMs）算出的 expire_at 会差 8 小时，
        // 同一条链路的两条记录互相比对时才知道不对。见 AppTime.now() 的注释。
        LocalDateTime now = AppTime.now();
        AtomicReference<String> reason = new AtomicReference<>();

        try {
            Boolean ok = tx.execute(status -> {
                QuotaRecord record = new QuotaRecord();
                record.setResourceId(res.getId());
                record.setDeptId(res.getDeptId());
                record.setStudentId(studentId);
                record.setStatus("LOCKED");
                record.setSource("FCFS");
                record.setClaimAt(now);
                record.setExpireAt(now.plusSeconds(confirmTimeoutSec));
                record.setCreatedAt(now);
                record.setUpdatedAt(now);

                // 返回值恒为 1；冲突时抛 DuplicateKeyException，由下面的 catch 接住。
                recordMapper.insertRecord(record);

                int updated = resourceMapper.deductRemain(res.getId(), now);
                if (updated == 0) {
                    reason.set("DB_REMAIN_ZERO");
                    status.setRollbackOnly();
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;
            });

            return new PersistOutcome(Boolean.TRUE.equals(ok), reason.get());
        } catch (DuplicateKeyException e) {
            return new PersistOutcome(false, "DUPLICATE");
        } catch (DataAccessException e) {
            log.error("落库约束失败 resourceId={} studentId={} reason={}",
                    res.getId(), studentId, e.getMostSpecificCause().getMessage());
            return new PersistOutcome(false, "DB_ERROR");
        }
    }

    /**
     * 回补。顺序是「先 INCR 再决定要不要删幂等键」：
     * 反过来的话，幂等键删了而 INCR 失败，学生会重新走一次扣减，但名额没还回来 —— 永久少一个。
     *
     * <p>INCR 自己失败时只能记 ERROR 交给对账（D9）。这类残余差异是分布式系统的常态，
     * 目标不是"永不出错"，而是"一定查得出来、修得回去"。
     */
    private void compensate(long resourceId, String studentId, boolean dropIdem) {
        try {
            redis.opsForValue().increment(RedisKeys.stock(resourceId));
            if (dropIdem) {
                redis.delete(RedisKeys.idem(resourceId, studentId));
            }
        } catch (Exception e) {
            log.error("回补 Redis 失败，需对账修复 resourceId={} studentId={}", resourceId, studentId, e);
        }
    }

    private record PersistOutcome(boolean ok, String reason) {
    }
}
