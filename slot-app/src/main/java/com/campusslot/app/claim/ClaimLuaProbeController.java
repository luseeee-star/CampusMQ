package com.campusslot.app.claim;

import com.campusslot.common.constant.RedisKeys;
import com.campusslot.common.constant.StudentIdRule;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * 单 key Redis 探针：<b>只跑 claim.lua</b>，不碰 DB、不碰 MQ。
 *
 * <p><b>它解决的是 D5 的归因问题。</b>「吞吐不再上升」这一种形状，至少有四种成因：
 * 压测器饱和、Tomcat 线程饱和、Redis 单 key 饱和、MQ 同步发送饱和。
 * 四条曲线在前半段都能长得一样。上一轮已经量了第一条（{@code /api/dev/ping}，
 * 保守 4500 req/s），这里量第三条——<b>剩下的差额才能归给 MQ</b>。
 *
 * <pre>
 *   /api/dev/ping        Tomcat + Spring MVC                    ← 客户端与容器的地板
 *   /api/dev/lua/claim   上一行 + Lettuce 往返 + claim.lua       ← Redis 单 key 的真实上限
 *   /api/claim/mq        上一行 + 元信息缓存 + 同步 MQ 发送       ← 业务链路
 * </pre>
 *
 * <p><b>为什么单独占一个 keyspace（默认 resourceId=9001）</b>：
 * 探针会真的 {@code DECR} 库存 key、真的写幂等键。若打在实验用的资源上，
 * 它会把那一轮的名额和幂等键搅乱——而 {@code /api/dev/stat} 的判据
 * （{@code remain + issued == totalQuota}、{@code idemKeys == records}）会立刻失效。
 * 用一个<b>没有 DB 行</b>的 id 之后，探针的一切副作用都关在自己的 key 里，
 * 而且它跑得再久也不会污染 {@code quota_resource}。
 *
 * <p>库存 key 在应用启动时灌一次（额度大到整轮扫描都不会见底），之后不再检查。
 * 每请求多判一次"key 在不在"就是多一次 Redis 往返，探针会立刻比真实路径还慢——
 * 插桩成本必须为 0，这一条比"自愈方便"重要得多。
 *
 * <p>返回纯文本而不是 JSON：JSON 序列化是 CPU 开销，会把这条线的上限压低一截，
 * 而那一截与 Redis 无关。真实接口（返回体更大）的上限只会更低，这个方向是安全的。
 * 带 {@link LongAdder} 命中计数，用于比对「客户端发出去了多少」与「服务端收到多少」——
 * 少了就说明客户端在丢请求，那一轮的吞吐是被低估的，不能当成 Redis 的上限。
 */
@RestController
@RequestMapping("/api/dev/lua")
@ConditionalOnProperty(name = "campusslot.dev-endpoints", havingValue = "true")
public class ClaimLuaProbeController {

    /** 探针默认使用的假 resourceId：没有对应的 {@code quota_resource} 行，副作用完全隔离。 */
    private static final long DEFAULT_PROBE_ID = 9001L;

    /** 探针库存自愈额度。要足够大，让整轮扫描都走「成功扣减」这条最重的路径。 */
    private static final long DEFAULT_SEED_QUOTA = 100_000_000L;

    /** 一次 DEL 最多带多少个 key，理由见 {@link #reset}。 */
    private static final int DELETE_BATCH = 500;

    private final StringRedisTemplate redis;
    private final RedisScript<List> claimScript;
    private final LongAdder hits = new LongAdder();

    /** 与 {@link #claimScript} 同一个文件，但**自己加载字节、自己算 SHA1**，不经过 bean 的任何方法。 */
    private static final String SCRIPT_LOCATION = "lua/claim.lua";

    /**
     * 启动时算好的脚本字节与 SHA1。请求路径只读这两个字段。
     *
     * <p>为什么不在请求里调 {@code claimScript.getSha1()}：那正是被测对象。
     * Spring Data Redis 的 {@code DefaultRedisScript.getSha1()} 每次调用都要拿一把
     * JVM 全局 {@code ReentrantLock}，即使 SHA1 早已缓存 —— 压测中抓的 {@code jstack} 里，
     * 400 个 Tomcat 线程有 393 个 WAITING 在这把锁上。所以这条路必须完全绕开它。
     */
    private volatile byte[] scriptBytes;
    private volatile String scriptSha1;

    private static final Logger log = LoggerFactory.getLogger(ClaimLuaProbeController.class);

    public ClaimLuaProbeController(StringRedisTemplate redis,
                                   @Qualifier("claimScript") RedisScript<List> claimScript) {
        this.redis = redis;
        this.claimScript = claimScript;
    }

    /**
     * 启动时把探针的库存 key 灌好。
     *
     * <p><b>绝不能在 {@link #claim} 里判"key 在不在再补"</b>：那会给每个请求多挂一次 Redis 往返，
     * 探针立刻变成"两次往返"的链路，测出来的数字比真实路径还慢。
     * 压测工具的插桩成本必须为 0，或者必须被明确计入——这里选前者。
     *
     * <p>只在启动时灌一次而不是每轮灌：库存掉了多少正好能反映这一轮跑了多少请求，
     * 是账目自洽的旁证。
     */
    @PostConstruct
    void init() {
        seedDefaultStock();
        initScriptCache();
    }

    /**
     * 自己加载脚本字节、自己算 SHA1，并核对它与 {@code claimScript} bean 是同一份脚本。
     *
     * <p>核对这一步不能省：EVALSHA 只认 SHA1，如果探针算的和 bean 指向的文件不是同一份，
     * 请求会全部 NOSCRIPT 后回退到 EVAL —— 既没测到东西，还不会报错，属于最难发现的那种失败。
     */
    private void initScriptCache() {
        try {
            byte[] bytes = new ClassPathResource(SCRIPT_LOCATION).getContentAsByteArray();
            String sha = sha1Hex(bytes);
            // 这一次调用发生在启动期、单线程，加锁无所谓；之后请求路径再也不碰它。
            String beanSha = claimScript.getSha1();
            if (beanSha != null && !beanSha.equals(sha)) {
                log.error("探针脚本与 claimScript bean 不是同一份：probe={} bean={}，EVALSHA 路径禁用", sha, beanSha);
                return;
            }
            this.scriptBytes = bytes;
            this.scriptSha1 = sha;
            log.info("探针 EVALSHA 就绪 sha1={} bytes={}", sha, bytes.length);
        } catch (Exception e) {
            log.warn("探针 EVALSHA 初始化失败，/claim-sha 将回退到 EVAL", e);
        }
    }

    /**
     * 启动时把探针的库存 key 灌好。
     *
     * <p><b>绝不能在 {@link #claim} 里判"key 在不在再补"</b>：那会给每个请求多挂一次 Redis 往返，
     * 探针立刻变成"两次往返"的链路，测出来的数字比真实路径还慢。
     * 压测工具的插桩成本必须为 0，或者必须被明确计入——这里选前者。
     *
     * <p>只在启动时灌一次而不是每轮灌：库存掉了多少正好能反映这一轮跑了多少请求，
     * 是账目自洽的旁证。
     */
    void seedDefaultStock() {
        try {
            if (Boolean.FALSE.equals(redis.hasKey(RedisKeys.stock(DEFAULT_PROBE_ID)))) {
                redis.opsForValue().set(RedisKeys.stock(DEFAULT_PROBE_ID),
                        String.valueOf(DEFAULT_SEED_QUOTA));
            }
        } catch (Exception e) {
            // Redis 没起来时不要让整个应用启动失败：探针是 dev 工具，不该有这种否决权
            log.warn("单 key 探针初始化失败（不影响主链路）", e);
        }
    }

    /**
     * 走和生产完全相同的 Lua 与相同的 key 结构，唯一区别是不发 MQ、不读元信息缓存。
     *
     * <p>脚本的 KEYS/ARGV 排布必须和 {@link AsyncClaimService#evalClaim} 一致，
     * 否则测出来的不是"Redis 那一层的成本"，而是一个只在这种形状下成立的数字。
     * 学院配额传 {@code "0"}：这一层要测的是单 key，加第二把 key 就变成两个变量。
     */
    @GetMapping("/claim")
    public String claim(@RequestParam(defaultValue = "9001") long resourceId,
                        @RequestParam String studentId,
                        @RequestParam(defaultValue = "600000") long idemTtlMs) {
        return runClaim(resourceId, studentId, idemTtlMs, false);
    }

    /**
     * 与 {@link #claim} 同一条链路、同一份脚本、同样的 key 结构，唯一差别是
     * <b>不经过 {@code DefaultScriptExecutor}</b>：用启动时算好的 SHA1 直接发 {@code EVALSHA}。
     *
     * <p>存在的理由只有一个：判定 5.8k 是被 Redis/Lua 限住的，还是被 Spring 的一把锁限住的。
     * 两个端点并存，就能在同一台机器、同一轮预热、同一个 JVM 里做 ABA 对照，
     * 把"换端点"和"换预热"两个变量分开 —— 这是前面几轮反复翻车的那个坑。
     *
     * <p><b>NOSCRIPT 回退必须保留</b>：Redis 重启或 {@code SCRIPT FLUSH} 之后脚本缓存是空的，
     * 不回退就是全部请求失败。回退走 {@code EVAL}，Redis 会顺带把脚本重新缓存进去，只付一次代价。
     */
    @GetMapping("/claim-sha")
    public String claimSha(@RequestParam(defaultValue = "9001") long resourceId,
                           @RequestParam String studentId,
                           @RequestParam(defaultValue = "600000") long idemTtlMs) {
        return runClaim(resourceId, studentId, idemTtlMs, true);
    }

    /**
     * 两条路径的共同部分。KEYS/ARGV 排布必须和主链路一致，见 {@link #claim} 的说明。
     */
    private String runClaim(long resourceId, String studentId, long idemTtlMs, boolean viaSha) {
        if (!StudentIdRule.isValid(studentId)) {
            // 探针也要拒绝：它写的是和主链路同构的幂等键，非法学号会把 key 结构打乱
            return "reject:" + StudentIdRule.rejectReason(studentId);
        }
        hits.increment();
        String stockKey = RedisKeys.stock(resourceId);
        // KEYS[3] 必须是"学院库存 key 的形状"，不能拿 stockKey 顶替：
        // ARGV[2]="0" 时 Lua 不会读它，所以顶替在功能上无害 —— 但它会让这个探针的
        // key 结构看起来和生产一致、实际不一致。D7 讨论分片时正是拿"三类 key 同槽"当依据，
        // 一个假 KEYS[3] 会让"要不要加 hash tag"的判断建立在错误前提上。
        List<String> keys = List.of(
                stockKey,
                RedisKeys.idem(resourceId, studentId),
                RedisKeys.collegeStock(resourceId, "C00"));
        String[] args = {String.valueOf(idemTtlMs), "0"};
        try {
            List<String> result = viaSha ? evalBySha(keys, args) : evalByScript(keys, args);
            if (result == null || result.isEmpty()) {
                return "empty";
            }
            // 必须把 code 一起带出去。之前只回 remain，所有业务码都变成 "ok:0"，
            // 压测器就只能数"脚本执行了多少次"，数不出"抢到了多少"。
            return "ok:" + result.get(0) + ":" + result.get(1);
        } catch (Exception e) {
            // 不让异常变成 500：500 会让 hammer 计入 error，看起来像"Redis 挂了"，
            // 而实际可能只是这一轮超时。用可识别的短文本，压测时一眼能分辨。
            return "err:" + e.getClass().getSimpleName();
        }
    }

    /**
     * 旧路径：{@code RedisTemplate.execute(RedisScript,...)} → {@code DefaultScriptExecutor}
     * → 每个请求都拿一次 {@code DefaultRedisScript.getSha1()} 的全局锁。这是对照组。
     */
    private List<String> evalByScript(List<String> keys, String[] args) {
        List<?> raw = redis.execute(claimScript, keys, (Object[]) args);
        List<String> out = new ArrayList<>();
        if (raw != null) {
            for (Object o : raw) {
                out.add(String.valueOf(o));
            }
        }
        return out;
    }

    /**
     * 新路径：直接 {@code EVALSHA}，请求期只读 {@link #scriptSha1} 这个 volatile 字段，无锁。
     *
     * <p>返回值是原始 {@code byte[]}（绕过了 template 的 valueSerializer），所以自己按 UTF-8 解。
     */
    @SuppressWarnings("unchecked")
    private List<String> evalBySha(List<String> keys, String[] args) {
        final String sha = this.scriptSha1;
        final byte[] bytes = this.scriptBytes;
        if (sha == null || bytes == null) {
            // 初始化失败（脚本读不到 / 与 bean 不一致）：退化成旧路径，端点仍可用，但不再声称无锁
            return evalByScript(keys, args);
        }
        Object raw = redis.execute((RedisCallback<Object>) conn -> {
            byte[][] encoded = encode(keys, args);
            try {
                return conn.evalSha(sha, ReturnType.MULTI, keys.size(), encoded);
            } catch (RuntimeException e) {
                if (!isNoScript(e)) {
                    throw e;
                }
                return conn.eval(bytes, ReturnType.MULTI, keys.size(), encoded);
            }
        });
        List<String> out = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object o : list) {
                out.add(o instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : String.valueOf(o));
            }
        }
        return out;
    }

    /** 小写十六进制 SHA1，格式与 Redis EVALSHA 期待的一致（框架内部也是这个格式）。 */
    private static String sha1Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(bytes);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM 不支持 SHA-1", e);
        }
    }

    private static byte[][] encode(List<String> keys, String[] args) {
        byte[][] out = new byte[keys.size() + args.length][];
        int i = 0;
        for (String k : keys) {
            out[i++] = k.getBytes(StandardCharsets.UTF_8);
        }
        for (String a : args) {
            out[i++] = a.getBytes(StandardCharsets.UTF_8);
        }
        return out;
    }

    /** Redis 重启或 SCRIPT FLUSH 后脚本缓存为空，EVALSHA 会报 NOSCRIPT —— 这种情况才允许回退。 */
    private static boolean isNoScript(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String m = t.getMessage();
            if (m != null && m.contains("NOSCRIPT")) {
                return true;
            }
        }
        return false;
    }

    /** 服务端实际执行的 Lua 次数，与 hammer 的 total 比对。 */
    @GetMapping("/count")
    public String count() {
        return String.valueOf(hits.sum());
    }

    /**
     * 清掉探针自己的库存与幂等键，让下一轮从同一个起点开始（只动 {@code resourceId} 指定的 key）。
     *
     * <p><b>分批 DEL，不要一次删十几万个 key</b>：{@code DEL k1 k2 ... kN} 是一个命令，
     * Redis 单线程执行它期间谁都得等。一轮扫描能写出十几万个幂等键，一条巨型 DEL 会在
     * <b>下一档的第一个采样窗口里</b>留一个几十毫秒的空白——而那个窗口正是我们要读吞吐的地方，
     * 于是"清场"这个动作会伪装成"这一档性能变差了"。每批 500 个，代价是几十次往返（毫秒级），
     * 换来的是清场不进入测量窗口。
     */
    @RequestMapping(value = "/reset", method = {RequestMethod.GET, RequestMethod.POST})
    public String reset(@RequestParam(defaultValue = "9001") long resourceId,
                        @RequestParam(defaultValue = "0") long quota) {
        long total = quota > 0 ? quota : DEFAULT_SEED_QUOTA;
        redis.opsForValue().set(RedisKeys.stock(resourceId), String.valueOf(total));
        hits.reset();
        int cleared = 0;
        List<String> batch = new ArrayList<>(DELETE_BATCH);
        try (Cursor<String> cursor = redis.scan(ScanOptions.scanOptions()
                .match(RedisKeys.idem(resourceId, "") + "*")
                .count(1000)
                .build())) {
            while (cursor.hasNext()) {
                batch.add(cursor.next());
                if (batch.size() >= DELETE_BATCH) {
                    cleared += deleteBatch(batch);
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            cleared += deleteBatch(batch);
        }
        return "quota=" + total + " idemCleared=" + cleared;
    }

    private int deleteBatch(List<String> batch) {
        Long removed = redis.delete(new HashSet<>(batch));
        return removed == null ? 0 : removed.intValue();
    }
}
