package com.campusslot.app.claim;

import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.RedisKeys;
import com.campusslot.common.entity.QuotaResource;
import com.campusslot.common.mapper.QuotaCollegeMapper;
import com.campusslot.common.mapper.QuotaResourceMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 资源元信息缓存：把申领链路上的最后一次 DB 往返也搬走。
 *
 * <p>D3 拆完之后，入口路径上应该<b>一次数据库访问都没有</b>：
 * Redis 判定（Lua）→ Redis 读元信息（本类）→ 投递 MQ。任何一次 DB 往返留在链路上，
 * 「异步落库省了多少」这个问题的答案里就会混着一个不含 MQ 收益的常数。
 *
 * <p><b>TTL 只有 5 秒（可配）</b>，不是 5 分钟。代价是每 5 秒每个活动一次主键查询，
 * 8000 QPS 下可以忽略；收益是运营把活动下架（PUBLISHED → OFFLINE）最多 5 秒就生效。
 * 缓存一小时的话，会出现「活动已经在管理端下线了，学生还在抢」——这类问题
 * 在压测里永远测不出来，只在演示现场出现。
 *
 * <p><b>不用 {@code @Cacheable}</b>：Spring Cache 的抽象在这里帮不上忙，
 * 反而要多维护一套序列化配置；而且缓存失效只有一个场景（dev reset），
 * 一个 {@link #evict(long)} 就够了。
 *
 * <p><b>为什么需要 single-flight（2026-11-10 补）</b>：5 秒 TTL 的失效点是<b>全局对齐</b>的——
 * 同一个活动只有一个 key，过期后<b>所有</b>在途请求同时变成 miss。8000 QPS 下这不是
 * "每 5 秒一次主键查询"，而是"每 5 秒几百个线程同时打一次主键查询"，两次 SQL
 * （{@code selectById} + {@code countByResource}）叠在一起，形成一个周期性的尖峰。
 * 它有两个具体后果，都不是理论问题：
 * <ul>
 *   <li>MySQL 的 {@code Questions} 曲线出现 5 秒周期的脉冲——压测时这条曲线会被读成
 *       "业务负载有周期性"，而实际上是缓存自己造出来的负载；</li>
 *   <li>尖峰正好落在连接池最紧的时刻（64 个连接），把 P99 的尾巴抬高，
 *       而这段时间的 RT 不属于"Redis 判定 + 投 MQ"的真实链路。</li>
 * </ul>
 * 合并之后：同一活动在缓存失效窗口内<b>只允许一个线程回源</b>，其余线程等它的结果。
 *
 * <p><b>失败的线程不排队等着，而是自己回源</b>（{@code follower_fallback}）。
 * 这是刻意的取法：single-flight 的目的是"削掉本可以共享的重复查询"，
 * 不是"把可用性绑在领导者身上"。领导者卡住（DB 慢、线程被换出）时，
 * 跟随者最多等 {@code single-flight-wait-ms}（默认 100ms）就自己查——
 * 最坏退化成今天的行为（N 个查询），不会退化成"所有人一起等一个卡住的线程"。
 * 用 {@code tryLock} 也能做到同样的事，但锁拿不到时仍然要自己查一次、
 * 且拿不到领导者的<b>结果</b>；future 能把这一个结果直接分给所有人。
 */
@Component
public class ResourceMetaCache {

    private static final Logger log = LoggerFactory.getLogger(ResourceMetaCache.class);

    private final StringRedisTemplate redis;
    private final QuotaResourceMapper resourceMapper;
    private final QuotaCollegeMapper collegeMapper;
    private final ObjectMapper json;
    private final long ttlMs;
    private final boolean collegeQuotaSwitch;
    private final MeterRegistry meters;

    /**
     * 跟随者等领导者的上限。取值理由：正常一次回源是「一次主键 + 一次 count」，1~3ms；
     * 100ms 是它的 30 倍，足够覆盖一次 GC 或一次行锁等待，又不至于让 400 个 Tomcat 线程
     * 在同一个 key 上叠着等——那种等待会把"缓存未命中"放大成"请求超时"。
     */
    private final long singleFlightWaitMs;

    /**
     * 正在回源的资源。key 是 {@code resourceId}，value 是那个线程承诺会完成的结果。
     * 用 {@code ConcurrentHashMap} 而不是 Guava/Caffeine 的 {@code LoadingCache}：
     * 只需要"同 key 合并"这一个语义，引入一个缓存库会顺带引入它自己的过期、
     * 刷新、权重策略，而本类的 TTL 语义必须和 Redis 侧完全一致（见类注释）。
     */
    private final ConcurrentHashMap<Long, CompletableFuture<ResourceMeta>> inflight = new ConcurrentHashMap<>();

    public ResourceMetaCache(StringRedisTemplate redis,
                             QuotaResourceMapper resourceMapper,
                             QuotaCollegeMapper collegeMapper,
                             ObjectMapper json,
                             MeterRegistry meters,
                             @Value("${campusslot.claim.meta-cache-ttl-ms:5000}") long ttlMs,
                             @Value("${campusslot.claim.meta-single-flight-wait-ms:100}") long singleFlightWaitMs,
                             @Value("${campusslot.claim.college-quota-enabled:false}") boolean collegeQuotaSwitch) {
        this.redis = redis;
        this.resourceMapper = resourceMapper;
        this.collegeMapper = collegeMapper;
        this.json = json;
        this.meters = meters;
        this.ttlMs = ttlMs;
        this.singleFlightWaitMs = Math.max(1, singleFlightWaitMs);
        this.collegeQuotaSwitch = collegeQuotaSwitch;
    }

    /** 命中返回快照，资源不存在返回 {@code null}（调用方据此回 1003）。 */
    public ResourceMeta get(long resourceId) {
        String key = RedisKeys.resourceMeta(resourceId);
        ResourceMeta cached = readCache(key, resourceId);
        if (cached != null) {
            count("hit");
            return cached;
        }
        return loadCoalesced(resourceId, key);
    }

    /** dev reset 改了 total_quota 之后必须调用，否则会拿旧的 totalQuota 去算余量。 */
    public void evict(long resourceId) {
        try {
            redis.delete(RedisKeys.resourceMeta(resourceId));
        } catch (Exception e) {
            log.warn("清除资源元信息缓存失败 resourceId={}", resourceId, e);
        }
    }

    /** 缓存读 + 反序列化。任何异常都退化成 miss，绝不把缓存故障放大成"资源不存在"。 */
    private ResourceMeta readCache(String key, long resourceId) {
        try {
            String cached = redis.opsForValue().get(key);
            if (cached == null) {
                return null;
            }
            return json.readValue(cached, ResourceMeta.class);
        } catch (Exception e) {
            // 缓存坏了（序列化格式变了、Redis 抖了一下）不能变成"资源不存在"，
            // 那会把一次缓存故障放大成整场活动 404。降级为回源 DB。
            log.warn("资源元信息缓存读取失败，降级回源 DB resourceId={}", resourceId, e);
            return null;
        }
    }

    /**
     * 未命中时合并回源：领头的线程查 DB 并写缓存，跟随它的线程直接拿同一个结果。
     *
     * <p>用 {@code putIfAbsent} 决定谁领头，而不是"先 computeIfAbsent 再判断谁创建的"——
     * 后者要靠额外标记区分创建者，多一个状态就多一种出错方式。
     */
    private ResourceMeta loadCoalesced(long resourceId, String key) {
        CompletableFuture<ResourceMeta> mine = new CompletableFuture<>();
        CompletableFuture<ResourceMeta> leader = inflight.putIfAbsent(resourceId, mine);
        if (leader == null) {
            count("miss_leader");
            try {
                ResourceMeta meta = loadFromDb(resourceId);
                writeCache(key, resourceId, meta);
                mine.complete(meta);
                return meta;
            } catch (RuntimeException e) {
                // 领导者失败要让跟随者立刻知道，否则它们要白等满 100ms 才各自回源
                mine.completeExceptionally(e);
                throw e;
            } finally {
                // remove(key, value) 而不是 remove(key)：避免把后来者的 future 摘掉
                inflight.remove(resourceId, mine);
            }
        }

        count("miss_follower");
        try {
            return leader.get(singleFlightWaitMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // 中断标记必须还原：Tomcat 线程是从池里复用的，吞掉中断会让下一次关停变迟滞
            Thread.currentThread().interrupt();
            count("follower_fallback");
            return loadAlone(key, resourceId);
        } catch (ExecutionException | TimeoutException e) {
            // 领导者失败或卡住：自己查，退化成没有合并时的行为（见类注释「失败的线程不排队」）
            count("follower_fallback");
            log.warn("资源元信息合并回源未拿到结果，本线程自行回源 resourceId={} waitMs={} cause={}",
                    resourceId, singleFlightWaitMs, e.toString());
            return loadAlone(key, resourceId);
        }
    }

    /** 不参与合并的回源：查 DB + 尽力写缓存。 */
    private ResourceMeta loadAlone(String key, long resourceId) {
        ResourceMeta meta = loadFromDb(resourceId);
        writeCache(key, resourceId, meta);
        return meta;
    }

    /**
     * 写缓存。<b>不缓存 {@code null}</b>：{@code null} 的语义是"资源不存在"，
     * 而资源是会被创建出来的——把一个 1003 缓存 5 秒，等于让人在 create 之后
     * 有 5 秒的窗口申请一个"不存在"的活动，而这 5 秒正是他收到创建成功回执之后。
     * 不缓存空值的代价是"对不存在的 id 每次回源"，这只在有人拿错 id 时才发生，
     * 属于该被看见的负载。
     */
    private void writeCache(String key, long resourceId, ResourceMeta meta) {
        if (meta == null) {
            return;
        }
        try {
            redis.opsForValue().set(key, json.writeValueAsString(meta), ttlMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("资源元信息缓存写入失败，本次请求继续 resourceId={}", resourceId, e);
        }
    }

    private ResourceMeta loadFromDb(long resourceId) {
        QuotaResource res = resourceMapper.selectById(resourceId);
        if (res == null) {
            return null;
        }
        boolean hasCollegeRows = collegeMapper.countByResource(resourceId) > 0;
        return new ResourceMeta(
                res.getId(),
                res.getDeptId() == null ? 0L : res.getDeptId(),
                res.getTotalQuota() == null ? 0 : res.getTotalQuota(),
                res.getStatus(),
                toEpochMs(res.getOpenAt()),
                toEpochMs(res.getCloseAt()),
                res.getConfirmTimeoutSec() == null ? 1800 : res.getConfirmTimeoutSec(),
                Boolean.TRUE.equals(res.getRosterRequired()),
                // 全局开关是 D12 决策点的开关：关掉它，学院行留着也不拦截，只出报表
                collegeQuotaSwitch && hasCollegeRows);
    }

    private static long toEpochMs(LocalDateTime time) {
        return AppTime.toEpochMs(time);
    }

    /**
     * 四种结果分开计数，因为它们的含义完全不同，混成一个 miss 率就看不出合并有没有生效：
     * {@code hit} / {@code miss_leader}（真的查了一次 DB）/ {@code miss_follower}（被合并掉的）/
     * {@code follower_fallback}（合并没兜住，退化成自己查）。
     * 稳态判据：{@code miss_follower >> miss_leader}，且 {@code follower_fallback ≈ 0}。
     */
    private void count(String result) {
        meters.counter("campusslot.claim.meta_cache", "result", result).increment();
    }
}
