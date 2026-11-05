package com.campusslot.common.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lua 脚本执行器：预计算 SHA1 + {@code EVALSHA}，绕开 Spring Data Redis 的一把全局锁。
 *
 * <p><b>为什么需要这个类</b>：{@code RedisTemplate.execute(RedisScript, ...)} 最终走
 * {@code DefaultScriptExecutor}，它在<b>每一次</b>执行前都会调用
 * {@code DefaultRedisScript.getSha1()}。那个方法为了返回缓存好的 sha1，
 * 仍然要先拿一把实例级 {@code ReentrantLock}：
 *
 * <pre>
 *   public String getSha1() {
 *       lock.lock();                 // ← 全局串行点，即使 sha1 已经算好了
 *       try { return sha1 != null ? sha1 : compute(); }
 *       finally { lock.unlock(); }
 *   }
 * </pre>
 *
 * <p>D7 实测：400 个 Tomcat 工作线程里 <b>393 个 WAITING 在这一把锁上</b>，
 * 吞吐被钉在 {@code 1/174μs ≈ 5.7k}；同一端点改用 {@code EVALSHA} 后 5.7k → 27.8k（×4.47，
 * ABA 对照 A/C 差 1.4%）。所以这不是"优化"，是把一个与业务无关的串行点摘掉。
 *
 * <p><b>NOSCRIPT 回退是必须的，不是防御性编程</b>：Redis 重启、{@code SCRIPT FLUSH}、
 * 主从切换都会清空脚本缓存，此时 EVALSHA 一律返回 NOSCRIPT。本类的处理是
 * {@code SCRIPT LOAD} 之后重试一次 EVALSHA。
 *
 * <p><b>为什么不用 {@code EVAL} 兜底 —— 理由不是"它不写缓存"</b>。
 * 2026-11-03 实测：{@code SCRIPT FLUSH} 后 {@code SCRIPT EXISTS}=0，
 * 执行一次 {@code EVAL} 后变成 1，随后 {@code EVALSHA} 直接命中。
 * 也就是说 {@code EVAL} 同样会写缓存，用它兜底并不会让回退变成常态。
 * 选 {@code SCRIPT LOAD} 的真实理由是：回退之后重试的仍然是 {@code EVALSHA}，
 * 与热路径是同一条代码路径，不存在"只在故障时才跑、因此没人测过"的第二条路径；
 * 而 {@code SCRIPT LOAD} 的返回值本身就是一次可校验的"缓存已恢复"信号。
 * （这条曾经写反过，改在这里：把理由写错比把理由写少危险，它会让人照着错误前提去改下一步。）
 *
 * <p><b>开关 {@code campusslot.redis.legacy-script-executor}</b>（默认 false）是<b>判决开关</b>，
 * 不是优化开关。打开它 = 请求交回 {@code RedisTemplate.execute(RedisScript,...)}
 * → {@code DefaultScriptExecutor} → 每个请求拿一次 {@code getSha1()} 的锁。
 * 它用来回答"锁在不在"，不是"报文大不大"。
 *
 * <p><b>这里曾经有一条把结论带偏的实现</b>：旧版本关闭开关时直接
 * {@code conn.eval(body, ...)} —— 那仍是裸 EVAL，<b>不经过框架，也就不拿锁</b>。
 * 于是 2026-11-03 那轮 A/B 的"旧路径"根本不是旧路径，两组都无锁，
 * 得出的"无收益"结论作废（详见 LOGBOOK D8 §3/§4）。现在关闭时真的走框架执行器，
 * 才有可能测出 5.7k 那把锁。
 *
 * <p>运行时可通过 {@code POST /api/claim/dev/lua-mode?legacy=true|false} 切换（仅 dev 端点开启时存在），
 * 目的是让 A/B 能在<b>同一个 JVM、同一个压测客户端</b>里交替进行 —— 换进程就是换冷启动，
 * 40% 的预热差会把 40% 的效应吃掉。
 *
 * <p><b>为什么按 {@link RedisScript} 实例缓存而不是按脚本路径</b>：bean 是单例，
 * 一个实例对应一份脚本，缓存命中是 O(1) 的 map 查找；按路径缓存还要多一次字符串比较，
 * 而这条路径每秒钟要走 2 万次。
 */
@Component
public class RedisScriptRunner {

    private static final Logger log = LoggerFactory.getLogger(RedisScriptRunner.class);

    private final StringRedisTemplate redis;
    private final boolean legacyAllowed;

    /**
     * 运行时可切：A/B 判决要求在同一个 JVM 里交替两条路径，构造函数里定死就只能靠重启，
     * 而重启换来的差异（预热、建连、JIT）比要测的效应还大。
     */
    private volatile boolean legacyMode;

    /** 脚本 → (sha1 bytes, body bytes)。只在首次执行时计算，之后纯 map 查找。 */
    private final Map<RedisScript<?>, Prepared> prepared = new ConcurrentHashMap<>();

    /** NOSCRIPT 回退次数。非 0 说明 Redis 侧发生过重启或 SCRIPT FLUSH。 */
    private final LongAdder fallbacks = new LongAdder();

    public RedisScriptRunner(StringRedisTemplate redis,
                             org.springframework.core.env.Environment env,
                             @Value("${campusslot.redis.legacy-script-executor:false}") boolean legacy,
                             @Value("${campusslot.redis.legacy-script-allowed:false}") boolean legacyAllowed) {
        this.redis = redis;
        this.legacyAllowed = legacyAllowed
                || env.acceptsProfiles(org.springframework.core.env.Profiles.of("bench"));
        requireAllowed(legacy);
        this.legacyMode = legacy;
        if (legacy) {
            log.warn("campusslot.redis.legacy-script-executor=true：脚本走 RedisTemplate 旧执行器，"
                    + "每请求拿一次 DefaultRedisScript.getSha1() 的锁，吞吐上限约 5.7k。"
                    + "只有做 A/B 判决时才该打开");
        }
    }

    /** 运行时可切的判决开关，见类注释。 */
    public boolean isLegacyMode() {
        return legacyMode;
    }

    /**
     * 切换执行路径。
     *
     * <p><b>非 bench 且未显式允许时直接抛</b>，而不是打个 warn 就放行：这个开关一旦在
     * 非压测环境被打开，表现是"系统莫名少了 40% 吞吐"，不会报错、不会告警、
     * 监控上一切都正常。这个项目已经踩过五次"配置了但没生效 / 生效了但没人知道"，
     * 所以在唯一入口上把它变成一条启动期异常。
     */
    public synchronized void setLegacyMode(boolean legacy) {
        if (legacy) {
            requireAllowed(true);
        }
        boolean before = this.legacyMode;
        this.legacyMode = legacy;
        if (before != legacy) {
            log.warn("脚本执行路径切换：{} → {}", before ? "legacy(框架锁)" : "evalsha(无锁)",
                    legacy ? "legacy(框架锁)" : "evalsha(无锁)");
        }
    }

    private void requireAllowed(boolean legacy) {
        if (legacy && !legacyAllowed) {
            throw new IllegalStateException(
                    "campusslot.redis.legacy-script-executor=true 只允许在 bench profile 下使用"
                            + "（当前 profile 不是 bench）。该开关会让每个请求去抢一把全局锁，"
                            + "吞吐上限约 5.7k；如果确实要复现旧口径，请加 --spring.profiles.active=bench，"
                            + "或显式设置 campusslot.redis.legacy-script-allowed=true 来表示你知道后果");
        }
    }

    /**
     * 执行脚本，返回字符串化的返回值列表。
     *
     * <p>返回值统一转成 {@code List<String>}：{@code StringRedisTemplate} 下所有调用方
     * 都是按字符串解析的（数字、nil 都对得上），强转 Long 会在 nil 截断时静默出错。
     */
    public List<String> eval(RedisScript<?> script, List<String> keys, String... args) {
        if (legacyMode) {
            return evalLegacy(script, keys, args);
        }

        Prepared p = prepare(script);

        try {
            return decode(exec(conn -> conn.evalSha(p.sha, ReturnType.MULTI, keys.size(), encode(keys, args))));
        } catch (RuntimeException e) {
            if (!isNoScript(e)) {
                throw e;
            }
            fallbacks.increment();
            log.warn("脚本缓存已清空（NOSCRIPT），SCRIPT LOAD 后重试 script={} fallbacks={}",
                    script.getClass().getSimpleName(), fallbacks.sum());
            return decode(exec(conn -> {
                conn.scriptLoad(p.body);
                return conn.evalSha(p.sha, ReturnType.MULTI, keys.size(), encode(keys, args));
            }));
        }
    }

    /**
     * 对照组：{@code RedisTemplate.execute(RedisScript,...)} → {@code DefaultScriptExecutor}。
     *
     * <p>这才是"旧路径"。它每请求都调 {@code script.getSha1()}，因此每请求都拿一次锁 ——
     * 这是本类唯一想暴露的差异，所以这里<b>不能</b>图省事写成 {@code conn.eval(...)}：
     * 那样是裸 EVAL，既不拿锁也不经过框架，测出来的是"报文大小差"，不是"锁差"。
     *
     * <p>用原始类型是一处刻意的妥协：{@code execute(RedisScript<T>, ...)} 的 {@code T}
     * 在 {@code RedisScript<?>} 上会捕获成通配符，无法直接接 {@code decode(List<?>)}。
     * 强转只发生在这一层，返回值仍走同一个 {@code decode}，不会出现两套解析。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<String> evalLegacy(RedisScript<?> script, List<String> keys, String[] args) {
        return decode((List<?>) redis.execute((RedisScript) script, keys, (Object[]) args));
    }

    /** NOSCRIPT 回退累计次数。压测期间非 0 就不是干净的一轮（有一次额外往返）。 */
    public long fallbackCount() {
        return fallbacks.sum();
    }

    private <T> T exec(RedisCallback<T> callback) {
        return redis.execute(callback);
    }

    private Prepared prepare(RedisScript<?> script) {
        return prepared.computeIfAbsent(script, s -> {
            String body = s.getScriptAsString();
            if (body == null || body.isBlank()) {
                throw new IllegalStateException("脚本内容为空，无法计算 SHA1: " + s);
            }
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            return new Prepared(sha1Hex(bodyBytes), bodyBytes);
        });
    }

    /**
     * KEYS 在前、ARGV 在后，按 Redis 协议拼成一个数组。
     *
     * <p>顺序错了不会报错，只会让脚本读到别的 key —— 这是最难查的一类脚本 bug，
     * 所以 {@code numKeys} 一律用 {@code keys.size()} 传，不在调用处手写数字。
     */
    private static byte[][] encode(List<String> keys, String[] args) {
        byte[][] out = new byte[keys.size() + args.length][];
        int i = 0;
        for (String k : keys) {
            out[i++] = k.getBytes(StandardCharsets.UTF_8);
        }
        for (String a : args) {
            out[i++] = a == null ? new byte[0] : a.getBytes(StandardCharsets.UTF_8);
        }
        return out;
    }

    private static List<String> decode(List<?> raw) {
        List<String> out = new ArrayList<>();
        if (raw == null) {
            return out;
        }
        for (Object o : raw) {
            if (o == null) {
                // MULTI 里 Lua 的 nil 元素会变成 null，而 String.valueOf(null) 得到的是
                // 字符串 "null" —— 调用方 parseInt 失败后走 fallback，"错误被静默吞掉"。
                // 用空串占位：保留位置（下标语义不能变），但不伪造一个看起来像值的字符串。
                out.add("");
            } else if (o instanceof byte[] b) {
                out.add(new String(b, StandardCharsets.UTF_8));
            } else {
                // MULTI 下整数会以 Long 返回（脚本里的 tonumber / DECR 结果）
                out.add(String.valueOf(o));
            }
        }
        return out;
    }

    /** 小写十六进制 SHA1，格式与 Redis EVALSHA 期待的一致。 */
    private static byte[] sha1Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(bytes);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 是 JDK 强制实现的算法，走不到这里；真走到了也不能静默降级
            throw new IllegalStateException("JDK 缺少 SHA-1", e);
        }
    }

    /**
     * 沿 cause 链找 NOSCRIPT。
     *
     * <p>只匹配 message 而不按异常类型判：Spring Data Redis 会把 Lettuce 的
     * {@code RedisCommandExecutionException} 包一层 {@code RedisSystemException}，
     * 不同版本包的层数还不一样，按类型判会在升级后静默失效 —— 表现是脚本永不回退、
     * Redis 一重启就全量报错。
     */
    private static boolean isNoScript(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String m = t.getMessage();
            if (m != null && m.contains("NOSCRIPT")) {
                return true;
            }
        }
        return false;
    }

    private record Prepared(byte[] sha, byte[] body) {
    }
}
