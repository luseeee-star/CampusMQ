package com.campusslot.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

/**
 * Redis Lua 脚本装配。
 *
 * <p>脚本上线前必须走 EVALSHA 缓存，避免每次全量传输脚本体。Spring Data Redis 的
 * {@link DefaultRedisScript} 会自动处理 EVAL/EVALSHA 回退。
 *
 * <p>{@code claimScript} 的返回类型是 {@code List} 而不是 {@code Long}：
 * v2.0 的 R-3 要求「重复请求返回首次结果」，脚本必须同时带回结果码和首次扣减后的余量。
 * 只返回一个数字的话，重复请求就只能回「你已参与过」，而这正是需求书 §4.2 明确要避免的。
 *
 * <p>注意元素类型取决于所用 RedisTemplate 的 valueSerializer —— {@code StringRedisTemplate} 下是 String，
 * 服务层统一按字符串解析，不要直接强转 Long。
 */
@Configuration
public class RedisScriptConfig {

    /**
     * claim_basic.lua：D2 的简化版，只有「幂等 + 总名额」两个判定。
     *
     * <p>它与 {@link #claimScript()} 同时存在是<b>故意的</b>：
     * D2 要回答的是「单加 Redis 原子扣减，超卖能降到 0 吗」，
     * 把 D4 的首次结果语义、G-1 的学院配额一起塞进去，就测不出这一层单独的作用了。
     * D2 跑完出数据后，主链路切到 claimScript，本方法保留作为降级开关与对照实现。
     */
    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RedisScript<List> claimBasicScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/claim_basic.lua"));
        script.setResultType(List.class);
        return script;
    }

    /** claim.lua：幂等判定 + 总名额扣减 + 学院配额扣减，一次原子操作。 */
    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RedisScript<List> claimScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/claim.lua"));
        script.setResultType(List.class);
        return script;
    }

    /**
     * compensate.lua：扣减之后任一步失败时的原子回补（INCR 总余量 + 可选 INCR 学院余量 + DEL 幂等键）。
     *
     * <p>返回 {@code {remain}} 而不是单个数字：见脚本头注释。用回补后的真实余量做日志，
     * 比只知道"补过了"有用——回补后余量和预期对不上，说明这期间有人同时在扣，账已经在漂了。
     */
    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RedisScript<List> compensateScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/compensate.lua"));
        script.setResultType(List.class);
        return script;
    }

    /**
     * release.lua：取消 / 超时释放时回补 Redis 镜像余量（INCR 总余量 + 可选 INCR 学院余量）。
     *
     * <p>与 {@link #compensateScript()} <b>不是同一个脚本，也不能合并</b>：补偿要 DEL 幂等键
     * （让学生能重试），释放必须保留幂等键（否则重复请求会走完整扣减 → 少发）。
     * 返回 {@code {code, remain}}，code 见脚本头：0 成功 / -1 库存 key 缺失 / -2 到上界。
     * 后两种必须落 {@code reconcile_diff}，不能只打日志。
     */
    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RedisScript<List> releaseScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/release.lua"));
        script.setResultType(List.class);
        return script;
    }

    /** token_bucket.lua：网关侧令牌桶，按学号 + IP 双维度。 */
    @Bean
    public RedisScript<Long> tokenBucketScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/token_bucket.lua"));
        script.setResultType(Long.class);
        return script;
    }
}
