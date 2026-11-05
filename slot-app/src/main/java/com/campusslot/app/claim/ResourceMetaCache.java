package com.campusslot.app.claim;

import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.RedisKeys;
import com.campusslot.common.entity.QuotaResource;
import com.campusslot.common.mapper.QuotaCollegeMapper;
import com.campusslot.common.mapper.QuotaResourceMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

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

    public ResourceMetaCache(StringRedisTemplate redis,
                             QuotaResourceMapper resourceMapper,
                             QuotaCollegeMapper collegeMapper,
                             ObjectMapper json,
                             @Value("${campusslot.claim.meta-cache-ttl-ms:5000}") long ttlMs,
                             @Value("${campusslot.claim.college-quota-enabled:false}") boolean collegeQuotaSwitch) {
        this.redis = redis;
        this.resourceMapper = resourceMapper;
        this.collegeMapper = collegeMapper;
        this.json = json;
        this.ttlMs = ttlMs;
        this.collegeQuotaSwitch = collegeQuotaSwitch;
    }

    /** 命中返回快照，资源不存在返回 {@code null}（调用方据此回 1003）。 */
    public ResourceMeta get(long resourceId) {
        String key = RedisKeys.resourceMeta(resourceId);
        try {
            String cached = redis.opsForValue().get(key);
            if (cached != null) {
                return json.readValue(cached, ResourceMeta.class);
            }
        } catch (Exception e) {
            // 缓存坏了（序列化格式变了、Redis 抖了一下）不能变成"资源不存在"，
            // 那会把一次缓存故障放大成整场活动 404。降级为回源 DB。
            log.warn("资源元信息缓存读取失败，降级回源 DB resourceId={}", resourceId, e);
        }

        ResourceMeta meta = loadFromDb(resourceId);
        if (meta == null) {
            return null;
        }
        try {
            redis.opsForValue().set(key, json.writeValueAsString(meta), ttlMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("资源元信息缓存写入失败，本次请求继续 resourceId={}", resourceId, e);
        }
        return meta;
    }

    /** dev reset 改了 total_quota 之后必须调用，否则会拿旧的 totalQuota 去算余量。 */
    public void evict(long resourceId) {
        try {
            redis.delete(RedisKeys.resourceMeta(resourceId));
        } catch (Exception e) {
            log.warn("清除资源元信息缓存失败 resourceId={}", resourceId, e);
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
}
