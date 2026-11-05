package com.campusslot.app.claim;

import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.constant.MqTopic;
import com.campusslot.common.constant.RedisKeys;
import com.campusslot.common.constant.StudentIdRule;
import com.campusslot.common.mq.ClaimMessage;
import com.campusslot.common.mq.ClaimMessageCodec;
import com.campusslot.common.mq.MqProducer;
import com.campusslot.common.mq.MqProperties;
import com.campusslot.common.redis.RedisScriptRunner;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 申领主链路（D3 + D4 版本）：Redis 判定 → 投递 {@code quota_claimed} → 立即返回。
 *
 * <p>与 D2 {@link ClaimService} 的差别只有一句话：<b>落库不再发生在请求线程里</b>。
 * <pre>
 *   D2  Lua → selectById → 事务(INSERT + UPDATE)
 *   D3  Lua → getMeta(Redis) → send(MQ)                    ← 入口零 DB 访问
 *       ↓
 *      cg_record 消费 → 事务(INSERT IGNORE + UPDATE remain)
 * </pre>
 *
 * <p><b>换成异步之后，唯一性从"事务"降级为"最终一致"。</b>D2 里「扣了 Redis 就一定写进 DB」
 * 由同一个请求线程保证；D3 里这两件事之间隔了一次网络投递和一段队列延迟。
 * 于是必须能回答三个问题，否则这次改造只是把 bug 从同步链路搬到了看不见的地方：
 *
 * <ol>
 *   <li><b>消息发出去了但落库失败，账还对得上吗？</b>
 *       消费端返回 {@code ConsumeResult.FAILURE} 让 MQ 重试；重试到上限仍失败就进死信队列。
 *       在此期间 Redis 已扣、DB 未扣，两者相差的数就是 {@code pending}，
 *       用 {@code /api/claim/dev/stat} 的 {@code pending} 字段可以随时读出来（正常应为 0 或正数）。</li>
 *   <li><b>消息重复投递会不会扣两次？</b>
 *       不会。消费端先 {@code INSERT IGNORE}，只有受影响行数为 1 才执行 {@code UPDATE remain-1}，
 *       两步在同一个事务里。第二次投递主键冲突 → 返回 0 → 直接跳过 UPDATE。</li>
 *   <li><b>投递本身失败怎么办？</b>
 *       回补 Redis（INCR 余量 + DEL 幂等键，见 {@code compensate.lua}）并返回 9003，
 *       学生的下一次点击是一次全新的请求，不会被幂等键判成"重复"。</li>
 * </ol>
 *
 * <p><b>那个无法消除的模糊态（一定要知道）</b>：同步发送超时时，客户端知道"没收到确认"，
 * 但不知道 Broker 到底收没收到。此时回补会让 {@code pending} 变成 <b>负数</b>
 * （Redis 认为没扣、DB 认为扣了）。这不是 bug，是分布式系统的固有窗口，
 * 处理方式是让对账（D9）能看见它并在报表里标注，而不是假装它不存在。
 * 换句话说：D3 之后，「账平不平」这件事从"靠事务保证"变成"靠对账发现"。
 *
 * <p><b>R-3 的落点：{@code CODE_REPLAY} 分支绝不能发消息。</b>
 * 重放意味着这次请求没有真的扣减（Lua 在判幂等时就返回了），
 * 若照样投递一条消息，消费端会 {@code INSERT IGNORE} 返回 0，然后按"重复"回补 Redis 余量——
 * 明明什么都没发生，余量却凭空多了一个。这就是"重放导致超卖"的经典写法，
 * 而且它只在真实学生重复点击时才触发，压测（每请求一个新学号）永远测不到。
 */
@Service
public class AsyncClaimService {

    private static final Logger log = LoggerFactory.getLogger(AsyncClaimService.class);

    // 与 claim.lua 的返回码一一对应，改脚本时必须同步改这里
    private static final int CODE_OK = 0;
    private static final int CODE_FULL = -1;
    private static final int CODE_REPLAY = -2;
    private static final int CODE_STOCK_MISSING = -3;
    private static final int CODE_COLLEGE_FULL = -4;

    /** 幂等键在「确认时限 + 冗余」之外的兜底存活时间：活动结束后还要活一天。 */
    private static final long IDEM_TTL_AFTER_CLOSE_MS = 24 * 3600 * 1000L;

    private final StringRedisTemplate redis;
    private final ResourceMetaCache metaCache;
    private final MqProducer producer;
    private final MqProperties mqProperties;
    private final RedisScriptRunner scriptRunner;
    private final RedisScript<List> claimScript;
    private final RedisScript<List> compensateScript;
    private final int idemTtlExtraSec;
    private final MeterRegistry meters;

    public AsyncClaimService(StringRedisTemplate redis,
                             ResourceMetaCache metaCache,
                             MqProducer producer,
                             MqProperties mqProperties,
                             RedisScriptRunner scriptRunner,
                             MeterRegistry meters,
                             @Qualifier("claimScript") RedisScript<List> claimScript,
                             @Qualifier("compensateScript") RedisScript<List> compensateScript,
                             @Value("${campusslot.claim.idem-ttl-extra-sec:600}") int idemTtlExtraSec) {
        this.redis = redis;
        this.metaCache = metaCache;
        this.producer = producer;
        this.mqProperties = mqProperties;
        this.scriptRunner = scriptRunner;
        this.meters = meters;
        this.claimScript = claimScript;
        this.compensateScript = compensateScript;
        this.idemTtlExtraSec = idemTtlExtraSec;
    }

    /**
     * MQ 没开时必须在启动日志里吵一次。
     *
     * <p>false 状态下 {@code MqProducer} 是只打日志的占位实现，接口会「看起来成功」，
     * 但消息从未进 Broker，一条记录都不会落库。压测照跑、数字照出，而全部作废。
     * 这类错误不可能从接口返回值上看出来，只能靠启动日志。
     */
    @PostConstruct
    void warnIfMqDisabled() {
        if (!mqProperties.isEnabled()) {
            log.error("!! campusslot.mq.enabled=false：/api/claim/mq 主链路不会真的落库。"
                    + "做 D3 对照必须用 --campusslot.mq.enabled=true 启动，否则全部数据作废。");
        }
    }

    public ClaimOutcome claim(long resourceId, String studentId) {
        // 入口就把非法学号挡掉：放过去的话，它会先扣掉一个 Redis 名额、写出幂等键、
        // 发出一条注定被 DB 拒掉的消息，然后由消费端重试到上限进死信。
        if (!StudentIdRule.isValid(studentId)) {
            return ClaimOutcome.fail(ErrorCode.PARAM_INVALID, StudentIdRule.rejectReason(studentId));
        }
        if (!mqProperties.isEnabled()) {
            // 与其"假装成功"，不如明确失败：异步路径离开 MQ 就没有意义
            return ClaimOutcome.fail(ErrorCode.MQ_UNAVAILABLE, "消息中间件未启用，无法受理申领");
        }

        ResourceMeta meta = metaCache.get(resourceId);
        if (meta == null) {
            return ClaimOutcome.fail(ErrorCode.RESOURCE_NOT_FOUND);
        }
        long nowMs = System.currentTimeMillis();
        if (!meta.isOpen(nowMs)) {
            return ClaimOutcome.fail(ErrorCode.RESOURCE_NOT_OPEN);
        }

        // 学院只在开了配额的活动上才查：默认路径零额外往返。
        // 查不到该学生 → 不在资格名单内（P4 前置），而不是"没有学院所以不限"。
        String college = null;
        if (meta.useCollegeQuota()) {
            college = readCollege(resourceId, studentId);
            if (college == null || college.isBlank()) {
                return ClaimOutcome.fail(ErrorCode.NOT_ELIGIBLE);
            }
        }

        int code;
        int remain;
        List<String> raw = evalClaim(resourceId, studentId, college, idemTtlMs(meta, nowMs));
        if (raw == null) {
            return ClaimOutcome.fail(ErrorCode.STOCK_CACHE_MISSING);
        }
        code = parseInt(raw, 0, Integer.MIN_VALUE);
        remain = parseInt(raw, 1, Integer.MIN_VALUE);
        if (code == Integer.MIN_VALUE) {
            log.error("claim.lua 返回值无法解析: {}", raw);
            return ClaimOutcome.fail(ErrorCode.SYSTEM_BUSY);
        }

        switch (code) {
            case CODE_OK -> {
                // 继续往下：投递消息
            }
            case CODE_REPLAY -> {
                // R-3：原样返回首次结果。这里**不能**发消息，理由见类注释最后一段。
                log.debug("幂等命中，重放首次结果 resourceId={} studentId={} firstRemain={}",
                        resourceId, studentId, remain);
                return ClaimOutcome.replay(remain);
            }
            case CODE_FULL -> {
                return ClaimOutcome.fail(ErrorCode.QUOTA_FULL);
            }
            case CODE_COLLEGE_FULL -> {
                // R-8 要求明确提示。这里绝不能复用 2001「名额已满」：
                // 页面会出现"名额已满"而进度条显示还有剩余，学生只会反复刷新投诉。
                return ClaimOutcome.fail(ErrorCode.COLLEGE_QUOTA_FULL);
            }
            case CODE_STOCK_MISSING -> {
                log.error("P0 库存 key 缺失 resourceId={} studentId={}", resourceId, studentId);
                return ClaimOutcome.fail(ErrorCode.STOCK_CACHE_MISSING);
            }
            default -> {
                log.error("claim.lua 返回未知码 {} resourceId={} studentId={}", code, resourceId, studentId);
                return ClaimOutcome.fail(ErrorCode.SYSTEM_BUSY);
            }
        }

        ClaimMessage message = new ClaimMessage(
                resourceId,
                meta.deptId(),
                studentId,
                college,
                nowMs,
                nowMs + meta.confirmTimeoutSec() * 1000L,
                ClaimMessage.SOURCE_FCFS);

        try {
            String msgId = producer.send(MqTopic.QUOTA_CLAIMED, message.messageKey(),
                    MqTopic.TAG_CLAIM, ClaimMessageCodec.encode(message));
            log.debug("已投递 quota_claimed msgId={} resourceId={} studentId={} remain={}",
                    msgId, resourceId, studentId, remain);
        } catch (Exception e) {
            // 投递失败 = 这次申领没有发生。必须回补，否则学生手里会有一个"抢到了但查不到"的名额。
            log.error("MQ 投递失败，回补 Redis resourceId={} studentId={}", resourceId, studentId, e);
            compensate(resourceId, studentId, college);
            return ClaimOutcome.fail(ErrorCode.MQ_UNAVAILABLE);
        }

        return ClaimOutcome.ok(remain);
    }

    /**
     * 幂等键 TTL：取「确认时限 + 冗余」与「活动结束 + 1 天」的较大者。
     *
     * <p>D2 用的是前者（40 分钟）。在异步链路下这个值不够：活动持续 7 天，
     * 学生在第 2 天重复点击时幂等键早就过期了，Lua 会当成一次全新申领再扣一次，
     * 直到消费端 {@code INSERT IGNORE} 返回 0 才发现是重复——那时余量已经被扣掉，
     * 必须回补，中间还多了一条无意义的消息。而且学生这次拿到的"余量"是错的。
     *
     * <p>把 TTL 拉到"活动结束 + 1 天"，重复请求在活动期间全部走快路径，
     * 真正做到 R-3 的"任何重复请求都返回首次结果"。内存代价：一个活动 10 万人约几 MB，
     * 活动结束一天后自动消失。
     */
    private long idemTtlMs(ResourceMeta meta, long nowMs) {
        long byConfirmTimeout = (meta.confirmTimeoutSec() + idemTtlExtraSec) * 1000L;
        long byActivityClose = meta.closeAtMs() - nowMs + IDEM_TTL_AFTER_CLOSE_MS;
        return Math.max(byConfirmTimeout, byActivityClose);
    }

    private String readCollege(long resourceId, String studentId) {
        try {
            return redis.opsForValue().get(RedisKeys.rosterEntry(resourceId, studentId));
        } catch (Exception e) {
            log.error("读取资格名单失败 resourceId={} studentId={}", resourceId, studentId, e);
            return null;
        }
    }

    /**
     * 执行 claim.lua。
     *
     * <p>返回 {@code null} 表示这次调用本身失败（Redis 挂了、脚本报错），
     * 与"脚本正常返回 -3"是两件事，但对外都收敛成 9005——因为它们对学生的含义相同：
     * 现在不能办，稍后再试。区别在日志。
     */
    private List<String> evalClaim(long resourceId, String studentId, String college, long idemTtlMs) {
        // KEYS 必须定长：学院未启用时传 stock 占位，靠 ARGV[2] 决定要不要用它。
        // 传空串也能跑，但空字符串 key 在慢日志和监控里看起来像配置错误，不如占位清楚。
        List<String> keys = List.of(
                RedisKeys.stock(resourceId),
                RedisKeys.idem(resourceId, studentId),
                college == null ? RedisKeys.stock(resourceId) : RedisKeys.collegeStock(resourceId, college));
        try {
            // 走 EVALSHA 而不是 redis.execute(script, ...)：后者每次执行都要拿
            // DefaultRedisScript.getSha1() 的全局锁（D7 实测 393/400 个线程排在它上面）。
            // 判决开关 campusslot.redis.legacy-script-executor（默认 false，仅 bench 可开）
            // 用于在同一进程内交替两条路径做 A/B，见 RedisScriptRunner 类注释。
            return scriptRunner.eval(claimScript, keys,
                    String.valueOf(idemTtlMs), college == null ? "0" : "1");
        } catch (Exception e) {
            // 绝不降级成"直接打 DB"：那会把整波流量灌到只有 64 个连接的池子上，
            // 从"抢不到"变成"全站不可用"。快速失败，交给前端重试与网关限流（D6）。
            log.error("Redis 执行 claim.lua 失败 resourceId={} studentId={}", resourceId, studentId, e);
            return null;
        }
    }

    /**
     * 原子回补。只回补 Redis，不碰 DB —— 因为 DB 侧根本还没有这条记录。
     *
     * <p>失败时只能记 ERROR 交给对账：这是分布式系统里正常的残余差异。
     * 目标不是"永不失败"，而是"一定查得出来、修得回去"。
     *
     * <p><b>三种结果必须分开记</b>（{@code campusslot.claim.compensate{result}}）：
     * 脚本返回 {@code {0}} 的语义是"库存 key 本来就不存在、脚本什么都没做"，
     * 而回补是 {@code INCR}，只增不减，<b>不可能</b>得到 0。也就是说
     * {@code remainAfterCompensate=0} 不可能是"回补成功、余量恰好为 0"，只可能是 key 缺失。
     * 两者以前打同一行 WARN，D10 注入 Redis 故障时会被读成"正常回补"。
     *
     * <p><b>为什么不落 {@code reconcile_diff}</b>：那张表的 {@code redis_remain / db_remain / drift}
     * 都是 NOT NULL，而补偿失败时恰恰<b>不知道</b> Redis 剩多少（INCR 根本没执行）。往 NOT NULL 列里
     * 编一个 0，就是一条看起来像数据的假证据——比"查不到"更难处理。改用计数器留痕，
     * D9 巡检加一条 {@code rate(campusslot_claim_compensate_total{result!="ok"}[5m])} 就能看见。
     */
    private void compensate(long resourceId, String studentId, String college) {
        List<String> keys = List.of(
                RedisKeys.stock(resourceId),
                RedisKeys.idem(resourceId, studentId),
                college == null ? RedisKeys.stock(resourceId) : RedisKeys.collegeStock(resourceId, college));
        try {
            List<String> result = scriptRunner.eval(compensateScript, keys, college == null ? "0" : "1");
            String value = (result == null || result.isEmpty()) ? null : result.get(0);
            if (value == null) {
                meters.counter("campusslot.claim.compensate", "result", "error").increment();
                log.error("P0 回补结果不可解析，名额可能永久少一个 resourceId={} studentId={} raw={}",
                        resourceId, studentId, result);
                return;
            }
            if ("0".equals(value.trim())) {
                // {0} = 库存 key 不存在，脚本直接返回，连幂等键都没 DEL。
                // 学生下一次请求会命中幂等键拿到"你已抢到"，而 Redis 里这个名额从未扣成——
                // 这是最难排查的一种悬空态，必须是 ERROR 而不是 WARN。
                meters.counter("campusslot.claim.compensate", "result", "stock_missing").increment();
                log.error("P0 回补未执行：库存 key 不存在，名额与幂等键均留在悬空态 resourceId={} studentId={}",
                        resourceId, studentId);
                return;
            }
            meters.counter("campusslot.claim.compensate", "result", "ok").increment();
            log.warn("已回补 Redis resourceId={} studentId={} remainAfterCompensate={}",
                    resourceId, studentId, value);
        } catch (Exception e) {
            meters.counter("campusslot.claim.compensate", "result", "error").increment();
            log.error("P0 回补失败，名额永久少一个，需对账修复 resourceId={} studentId={}",
                    resourceId, studentId, e);
        }
    }

    private static int parseInt(List<String> raw, int index, int fallback) {
        if (raw.size() <= index || raw.get(index) == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(String.valueOf(raw.get(index)).trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
