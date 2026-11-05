import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 抢名额压测器：制造并发抢名额，数成功 / 已满 / 异常，出 RT 分位。
 *
 * <p>为什么不用 Gatling：D1/D2 只需要「固定并发打一个接口」。Gatling 的 Scala DSL 与容器编排
 * 在这个阶段换不来任何东西，而它真正值钱的部分（阶跃加压、自动出曲线）要到 D5 才用得上。
 * JDK 21 支持单文件源码直接运行，零依赖零构建，改一个数字就能重跑。
 *
 * <p><b>同一个压测器跑两组对照组</b>，只改 path 参数。这不是图省事：
 * 两份压测器一旦在「线程怎么起、起跑线怎么同步、RT 从哪一刻开始算」上有一点不一致，
 * 两轮的数据就不可比，而这类差异肉眼看不出来。
 *
 * <p>用法（在仓库根目录执行）：
 * <pre>
 *   # 对照组① D1 裸 DB
 *   java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3
 *   # 对照组② D2 Redis Lua 同步落库
 *   java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3 1 /api/claim resourceId
 *   # 对照组③ D3 Redis + MQ 异步落库
 *   java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3 1 /api/claim/mq resourceId
 *   # 组⓪ 压测器自身上限（空转，不碰 DB / Redis / MQ）
 *   java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 5 0 /api/dev/ping none
 *   #                                                            ↑base  ↑并发 ↑每线程次数 ↑id  ↑path           ↑参数名
 *
 *   # D5 阶跃加压：每档压 6 秒，逐档加并发，并逐档校验服务端命中数
 *   java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 0 0 9001 \
 *        /api/dev/lua/claim resourceId \
 *        --sweep=50,100,200,400,800 --seconds=6 --count=/api/dev/lua/count
 *
 *   # D5 开环：固定速率灌，测可用性边界（NFR-2 / NFR-6）
 *   java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 0 0 1 \
 *        /api/claim/mq resourceId \
 *        --qps=1000,2000,4000,8000 --seconds=20 --threads=256 \
 *        --reset='/api/claim/dev/reset?resourceId=1&quota=500000'
 * </pre>
 *
 * <p><b>阶跃模式（{@code --sweep}）与按次数模式的区别只有一处：窗口怎么定。</b>
 * 按次数压，50 并发要跑 8 秒、800 并发只要 0.6 秒——后者测到的是 JIT 和建连的瞬时值，
 * 两档放进同一张曲线图没有可比性。阶跃模式固定时长、闭合环，每档都在同一个时间窗口里比。
 * 两种模式的分位数都是按真实样本算的，同一轮扫描内部可比，跨模式不要混着看。
 *
 * <p>跑完必须再查一次判据，只看压测器自己的"成功数"是不够的：
 * <pre>
 *   curl ".../api/baseline/stat?activityId=1"     D1：判据 remain &lt; 0
 *   curl ".../api/claim/dev/stat?resourceId=1"    D2/D3：判据 quotaInvariant &amp;&amp; pending == 0
 *                                                 D3 还要先 /drain 等 pending 归 0
 * </pre>
 * 注意三组判据<b>不是一个</b>：
 * <ul>
 *   <li>D1 的裸表允许 remain 为负，超卖就是负数本身</li>
 *   <li>D2/D3 的主表有 CHECK(remain &gt;= 0)，超卖只能表现为「record 数超过名额」，
 *       而「不超过名额」这件事的稳定表述是 {@code remain + issued == totalQuota}。
 *       不要用 {@code records > totalQuota} —— D8 引入取消之后，累计 records
 *       合法地可以远超名额（发了取消、取消再发），那个判据会天天误报</li>
 *   <li>D3 多了异步维度：压测结束时 pending 一定 &gt; 0，不等它就下结论会把"还在队列里"读成"丢消息"</li>
 * </ul>
 */
public class BaselineHammer {

    private static final String DEFAULT_PATH = "/api/baseline/claim";
    private static final String DEFAULT_ID_PARAM = "activityId";

    public static void main(String[] args) throws Exception {
        String base = args.length > 0 ? args[0] : "http://127.0.0.1:8080";
        int concurrency = args.length > 1 ? Integer.parseInt(args[1]) : 200;
        int perThread = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        long targetId = args.length > 3 ? Long.parseLong(args[3]) : 1L;
        String path = args.length > 4 ? args[4] : DEFAULT_PATH;
        String idParam = args.length > 5 ? args[5] : DEFAULT_ID_PARAM;

        String url = base.replaceAll("/+$", "") + path;
        int total = concurrency * perThread;
        boolean baselineMode = path.startsWith("/api/baseline");
        boolean asyncMode = path.contains("/mq");
        // 空转模式：只测「这个压测器自己能把多少请求打出去」，链路上没有 DB / Redis / MQ。
        // 用来给 D5 找 Redis 拐点定一条下限——拐点必须明显低于这条线的峰值，
        // 否则测到的就是压测器的天花板，而不是 Redis 的。
        boolean pingMode = path.contains("/ping");
        String groupName = pingMode ? "group 0: client ceiling (no-op, HTTP only)"
                : baselineMode ? "group 1: bare DB"
                : asyncMode ? "group 3: redis + mq async persist"
                : "group 2: redis lua sync persist";

        // ---- D5：阶跃加压。有 --sweep 就走闭合环按时长压，不再按"每线程几次" ----
        // 为什么必须是按时长而不是按次数：同一组请求数，在 50 并发下要跑 8 秒、在 800 并发下
        // 只要 0.6 秒。后者测到的是 JIT 与建连的瞬时值，不是稳态吞吐，两档放在一张曲线图上
        // 没有可比性。时长固定，每一档都在同样的时间窗口里比。
        String sweepSpec = option(args, "--sweep");

        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        if (sweepSpec != null && !sweepSpec.isBlank()) {
            String secondsOpt = option(args, "--seconds");
            long windowSec = Long.parseLong(secondsOpt == null ? "6" : secondsOpt);
            String countPath = option(args, "--count");
            String resetPath = option(args, "--reset");
            String drainPath = option(args, "--drain");
            String stepToggleSpec = option(args, "--step-toggle");
            runSweep(client, base, path, url, idParam, targetId, pingMode,
                    groupName, sweepSpec, windowSec, countPath, resetPath, drainPath, stepToggleSpec);
            return;
        }

        // ---- D5 补齐：开环（固定 QPS）----
        // 与闭合环的区别不是实现细节，而是回答的问题不同：
        //   闭合环 = 「N 个人同时在点」，QPS 是结果，服务端慢了自然少发 → 失败率永远接近 0
        //   开环   = 「上游按 N/s 灌我」，QPS 是输入，服务端慢了就排队、就超时 → 能测可用性边界
        // NFR-2（失败率 < 0.5%）和 NFR-6（8000 QPS）只有开环能测。
        String qpsSpec = option(args, "--qps");
        if (qpsSpec != null && !qpsSpec.isBlank()) {
            long windowSec = Long.parseLong(option(args, "--seconds") == null ? "10" : option(args, "--seconds"));
            int poolSize = Integer.parseInt(option(args, "--threads") == null ? "256" : option(args, "--threads"));
            String resetPath = option(args, "--reset");
            String drainPath = option(args, "--drain");
            runOpenLoop(client, base, url, idParam, targetId, pingMode, groupName,
                    qpsSpec, windowSec, poolSize, resetPath, drainPath);
            return;
        }

        AtomicInteger ok = new AtomicInteger();
        AtomicInteger full = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        AtomicInteger err = new AtomicInteger();
        AtomicLong[] rts = new AtomicLong[total];
        for (int i = 0; i < total; i++) {
            rts[i] = new AtomicLong(-1);
        }
        AtomicInteger slot = new AtomicInteger();

        // 所有线程在同一条起跑线上放行，否则先起的线程早跑完，
        // 并发度变成串行，超卖根本造不出来
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);

        for (int t = 0; t < concurrency; t++) {
            final int tid = t;
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // 每个请求用不同学号：模拟 1000 个不同学生，
                        // 而不是同一个人狂点（后者会被唯一索引挡，测不到超卖）
                        String sid = "S" + tid + "_" + i + "_" + System.nanoTime();
                        // 空转模式不带任何查询参数：ping 端点不读 resourceId / studentId，
                        // 带上去只会给 URL 解析多加点无谓的工作，把要测的上限压低一点点。
                        String fullUrl = pingMode ? url
                                : url + (url.contains("?") ? "&" : "?")
                                + idParam + "=" + targetId + "&studentId=" + sid;
                        long t0 = System.nanoTime();
                        // 构造 HttpRequest 也必须在 try 里：
                        // URL 拼错时 URI.create 抛的是 IllegalArgumentException，
                        // 留在 try 外面会让线程直接死掉 —— err 不计数、RT 不记录，
                        // 而统计结果显示 error=0，看起来像"压测跑得很好"。
                        try {
                            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(fullUrl))
                                    .timeout(Duration.ofSeconds(10));
                            HttpRequest req = pingMode
                                    ? builder.GET().build()
                                    : builder.POST(HttpRequest.BodyPublishers.noBody()).build();
                            HttpResponse<String> resp = client.send(req,
                                    HttpResponse.BodyHandlers.ofString());
                            long rt = (System.nanoTime() - t0) / 1_000_000;
                            rts[slot.getAndIncrement()].set(rt);
                            String body = resp.body() == null ? "" : resp.body();
                            if (pingMode) {
                                // 空转模式不看业务码，只看 HTTP 是否真的回来了
                                if (resp.statusCode() == 200) {
                                    ok.incrementAndGet();
                                } else {
                                    other.incrementAndGet();
                                }
                            } else if (body.contains("\"code\":0")) {
                                ok.incrementAndGet();
                            } else if (body.contains("\"code\":2001")) {
                                full.incrementAndGet();
                            } else {
                                other.incrementAndGet();
                            }
                        } catch (Exception e) {
                            long rt = (System.nanoTime() - t0) / 1_000_000;
                            rts[slot.getAndIncrement()].set(rt);
                            err.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "hammer-" + t);
            worker.setDaemon(true);
            worker.start();
        }

        long wallStart = System.nanoTime();
        start.countDown();
        done.await();
        long wallMs = (System.nanoTime() - wallStart) / 1_000_000;

        long[] sorted = Arrays.stream(rts).mapToLong(AtomicLong::get)
                .filter(v -> v >= 0).sorted().toArray();

        System.out.println();
        System.out.println("=== " + groupName + " ===");
        System.out.println("target      " + url);
        System.out.println("concurrency " + concurrency + "   per-thread " + perThread
                + "   total " + total);
        System.out.println("wall        " + wallMs + " ms"
                + "   throughput " + (wallMs == 0 ? "-" : (total * 1000L / wallMs) + " req/s"));
        System.out.println((pingMode ? "ok(200)     " : "ok(locked)  ") + ok.get());
        System.out.println("full        " + full.get());
        System.out.println("other-code  " + other.get());
        System.out.println("error       " + err.get());
        if (sorted.length > 0) {
            System.out.println("rt ms       p50 " + pct(sorted, 50)
                    + "   p95 " + pct(sorted, 95)
                    + "   p99 " + pct(sorted, 99)
                    + "   max " + sorted[sorted.length - 1]);
        }
        System.out.println();
        String baseUrl = base.replaceAll("/+$", "");

        if (pingMode) {
            // 空转模式没有业务判据，只有一条：服务端实际收到的请求数要等于 total。
            // 少了说明客户端侧有请求根本没发出去（连接被拒 / 超时未计入），
            // 那这一轮的 throughput 是被低估的，拿它当上限会低估压测器的真实能力。
            System.out.println("next: curl \"" + baseUrl + "/api/dev/ping/count\"");
            System.out.println("      check: 命中数应 == total(" + total
                    + ")，少了就是客户端侧丢请求，这一轮吞吐不可信");
            System.out.println("      use  : 扫 concurrency 画曲线，取峰值作为压测器上限；"
                    + "Redis 拐点必须明显低于它");
            System.out.println();
            return;
        }

        String statUrl = baselineMode
                ? "/api/baseline/stat?activityId=" + targetId
                : "/api/claim/dev/stat?resourceId=" + targetId;
        if (asyncMode) {
            // 异步落库跑完那一刻 records 一定比 ok 少，这是队列延迟不是丢消息。
            // 先等 pending 归 0 再下结论，否则会把"还在队列里"读成"丢了 40 条"。
            System.out.println("wait: curl \"" + baseUrl + "/api/claim/dev/drain?resourceId=" + targetId + "\"");
        }
        System.out.println("next: curl \"" + baseUrl + statUrl + "\"");
        System.out.println(baselineMode
                ? "      judge: remain < 0  (oversold = -remain)"
                : asyncMode
                ? "      judge: pending == 0 && quotaInvariant == true && oversold <= 0"
                : "      judge: quotaInvariant == true (remain + issued == totalQuota) && drift == 0");
        if (asyncMode) {
            System.out.println("      note : pending < 0 表示 DB 比 Redis 多扣，属 D9 对账范围，不是超卖");
        }
        System.out.println();
    }

    /** {@code --name=value} 形式的选项；不存在返回 null。 */
    private static String option(String[] args, String name) {
        String prefix = name + "=";
        for (String a : args) {
            if (a != null && a.startsWith(prefix)) {
                return a.substring(prefix.length());
            }
        }
        return null;
    }

    /**
     * NFR-2 的口径：失败率 = err / 总请求数。
     *
     * <p>{@code full}（2001 名额已满）和 {@code other}（名单外 1005、学院配额满 2002 等）
     * 都是<b>业务拒绝</b>，不属于系统失败。把它们算进失败率，超卖压测的分母一饱和就会
     * 让失败率恒在 90% 以上，那个数字既不对应可用性，也没法和 NFR-2 的 0.5% 比。
     *
     * <p>但 {@code other} 必须单独可见：它是「有请求没被正确受理」的信号，
     * 归到失败里会污染 NFR-2，藏起来则会漏掉真实故障。
     *
     * <p>用 {@code Locale.ROOT} 格式化：默认 locale 在部分环境用逗号当小数点，
     * 输出会变成 {@code 0,00}，CSV 直接多出一列，而肉眼看起来毫无异常。
     */
    private static String failPct(StepStats st) {
        int total = st.total();
        if (total == 0) {
            return "0.00";
        }
        return String.format(java.util.Locale.ROOT, "%.2f", 100.0 * st.err.get() / total);
    }

    /**
     * 「含客户端丢弃」的失败率：分子加上 {@code rej}，分母加上 {@code rej}。
     *
     * <p>为什么必须单独有这一列：开环 8000 档实测 {@code err=0} 而 {@code rej=3981/5s}。
     * 只报 {@code failPct} 会得到 0.00%，读起来是"NFR-2 达标"；但从学生角度看，
     * 点了没反应、最后超时，和返回 500 没有区别 —— 区别只在"谁丢的"。
     *
     * <p><b>只把 {@code rej} 加进分母是无效修改</b>：{@code err} 是 0，0/39870 仍是 0.00%。
     * 必须同时进分子，这个数才会从 0 变成正的。
     *
     * <p>闭合环下 {@code rej} 恒为 0，这一列与 {@link #failPct} 恒等，可以直接对比确认没算错。
     */
    private static String failPctAll(StepStats st) {
        long rejected = Math.max(0, st.rejected);
        long all = st.total() + rejected;
        if (all == 0) {
            return "0.00";
        }
        return String.format(java.util.Locale.ROOT, "%.2f", 100.0 * (st.err.get() + rejected) / all);
    }

    /** 记一个 RT 样本：分位数用队列，均值用累加。两处都要写，别只写一处。 */
    private static void recordRt(StepStats st, long sinceNanos) {
        long rt = (System.nanoTime() - sinceNanos) / 1_000_000;
        st.rts.add(rt);
        st.rtSum.addAndGet(rt);
    }

    /** 平均 RT。样本为 0 时返回 0，避免除零把整张表变成 NaN。 */
    private static long avgRt(StepStats st) {
        int n = st.rts.size();
        return n == 0 ? 0 : st.rtSum.get() / n;
    }

    /**
     * 真实完成速率：{@code total / 线程池排空耗时}。
     *
     * <p>与 {@code thr}（{@code total / 窗口时长}）的区别只在分母，但结论可能完全相反：
     * 窗口 5 秒、p50 2.4 秒的档位，最后一批请求必然在窗口结束后才回来，
     * {@code thr} 把它们算进了分子却没算进分母。两者相等才说明「窗口内真的收下了」。
     */
    private static long thrReal(StepStats st) {
        if (st.lastFinishMs <= 0) {
            return -1;   // 没排空，分母未知，只给下界
        }
        return st.total() * 1000L / st.lastFinishMs;
    }


    /** 记录第一个非预期响应的样本（只记一条，避免在高并发下抢 String 拼接的锁）。 */
    private static void recordBad(StepStats st, int status, String body) {
        String snippet = body == null ? "" : body.replaceAll("\\s+", " ");
        if (snippet.length() > 120) {
            snippet = snippet.substring(0, 120) + "...";
        }
        st.firstBad.compareAndSet(null, "HTTP " + status + " " + snippet);
    }

    /** 一档的全部观测值。每次跑新档都新建一个，避免上一档的计数漏进下一档。 */
    private static final class StepStats {
        final AtomicInteger ok = new AtomicInteger();
        final AtomicInteger full = new AtomicInteger();
        final AtomicInteger other = new AtomicInteger();
        final AtomicInteger err = new AtomicInteger();
        /**
         * D6：被入口限流拒绝（HTTP 429 / code 3001）。
         *
         * <p>必须独立成一项，不能并进 ok 也不能并进 err：
         * 并进 ok 会让限流后的"吞吐"看起来还在涨，其实全是拒绝；
         * 并进 err 会让 NFR-2 失败率虚高 —— 而需求书 §7 明确规定主动拒绝不计入失败率。
         * 它也不等于 {@link #rejected}：rejected 是客户端线程池没排上，这个是服务端真的回了 429。
         */
        final AtomicInteger limited = new AtomicInteger();
        /**
         * 探针专用：Lua 返回 {@code -2}，即幂等命中。
         *
         * <p>为什么要单独一列：{@code -2} 的 RT 和 {@code 0} 几乎一样（都在一次 EVAL 里），
         * 但语义完全不同 —— 前者根本没扣库存。混进 {@code ok} 会让"抢到名额"的数量虚高，
         * 而混进 {@code other} 又会让压测看起来像在报错。D7 讨论过的 12,717 缺口，
         * 当时因为没有这一列，只能靠猜：它到底是"被拒"还是"重复请求"。
         */
        final AtomicInteger idem = new AtomicInteger();
        /**
         * 用队列而不是预分配数组：按次数压时总数是已知的（{@code concurrency × perThread}），
         * 按时长压时总数事先不知道，只能边跑边收。装箱开销相对一次 HTTP 往返可以忽略，
         * 换来的是分位数仍然按真实样本计算，不是估算的直方图。
         */
        final ConcurrentLinkedQueue<Long> rts = new ConcurrentLinkedQueue<>();
        /**
         * 第一个"不是预期结果"的响应样本。
         *
         * <p>没有这个字段的时候踩过一次坑：探针是 GET，压测器按 POST 打过去全部 405，
         * 而 405 的响应体是 Spring 的错误 JSON —— 不含 {@code "code":0}，于是 15000 个请求
         * 全被记成 {@code other}，成功数 0，RT 却正常。看上去像"接口全挂了"，实际是方法用错。
         * 记一条原始样本，这种问题一眼就能看出来。
         */
        final java.util.concurrent.atomic.AtomicReference<String> firstBad =
                new java.util.concurrent.atomic.AtomicReference<>();
        long wallMs;
        int serverHits = -1;
        /**
         * 开环专用：本档「按节拍应该发起」的请求数。闭合环下等于 total()，
         * 开环下它才是输入（QPS × 窗口），而 total() 只是「回来的」那部分。
         * 两者之差 = 还在池子里 / 被拒，混在一起会把过载读成「压测器发不出去」。
         */
        long offered = -1;
        /** 开环专用：线程池队列满而没能提交的请求数。不阻塞节拍器是开环的前提。 */
        long rejected;
        /** 开环专用：窗口结束时仍未完成的请求数（服务端已积压到超过队列+线程容量）。 */
        long inflight;
        /**
         * 闭合环专用：本档「在途请求数」的时间加权平均。
         *
         * <p><b>为什么需要它</b>：闭合环里 {@code 吞吐 = 并发 ÷ RT} 是恒等式，
         * 拿它做归因是循环论证。真正能证伪「RT 里有一部分不是服务成本」的，
         * 是 {@code N = X × R} 是否闭合 —— 即实测平均在途数 × 实测 RT 是否等于实测吞吐。
         * {@link #inflight} 只在开环有意义（窗口结束时还在途的数量），
         * 而闭合环要的是**整窗口的时间加权平均**，两者语义不同，不能共用一个字段。
         *
         * <p><b>平均怎么算</b>：{@code ∫N dt / T}）。用固定间隔采样近似积分，
         * 而不是「请求进出累计次数」—— 后者算出来是并发数的两倍平均值，不是同时在途数。
         */
        long inflightAvg;
        /** 闭合环专用：采样到的在途请求数峰值。与 avg 一起看能判断窗口内是否有爬升/塌陷。 */
        int inflightPeakMax;
        /**
         * RT 总和，只为算均值。
         *
         * <p>中位数和均值一起看才能判断「窗口内完成」是否成立：若均值远大于中位数，
         * 说明尾部有少量极慢请求；若均值反而不大，说明样本集中在均值附近，长尾不存在。
         */
        final java.util.concurrent.atomic.AtomicLong rtSum = new java.util.concurrent.atomic.AtomicLong();
        /**
         * 开环专用：从窗口起点到「线程池彻底排空」的毫秒数。
         *
         * <p>它是 {@code thr} 的分母校验。{@code thr = total / 窗口时长}，但窗口结束后
         * 队列里可能还压着一堆没跑完的请求，{@code awaitTermination} 会等它们跑完，
         * 这段时间不计入分母 —— 于是 {@code thr} 报的是「窗口内发出去多少」，
         * 而不是「每秒真正处理完多少」。{@code total / lastFinishMs} 才是后者。
         *
         * <p>两者差得远，说明这一档的数字是注入速率，不是吞吐。
         * {@code -1} 表示 60 秒内没排空，此时分母未知，该档只能给下界。
         */
        long lastFinishMs = -1;

        int total() {
            return ok.get() + full.get() + other.get() + err.get() + limited.get();
        }
    }

    /**
     * 阶跃加压：一档一档往上加并发，每档压固定时长，记录稳态吞吐与分位。
     *
     * <p><b>为什么是闭合环（closed loop）而不是开环恒定 QPS</b>：本机没有独立施压机，
     * 开环需要一个精确的发送节拍器，而节拍器自己就是 CPU 争用的一部分。
     * 闭合环的语义是「这么多学生同时在点」——它会把服务端的拥塞直接变成响应时间变长，
     * 正是我们要看的东西。代价是：饱和之后「加压」不再真的增加压力，
     * 所以曲线上「吞吐不再上升」只说明到了容量上限，不能反推「QPS 只能到这么多」之外的结论。
     *
     * <p><b>每档都要先清服务端计数再压</b>：不清的话 {@code hit} 是累加值，
     * 与这一档的 total 比对必然对不上，而你会以为客户端在丢请求。
     */
    private static void runSweep(HttpClient client, String base, String path, String url, String idParam,
                                 long targetId, boolean pingMode, String groupName,
                                 String sweepSpec, long windowSec, String countPath,
                                 String resetPath, String drainPath, String stepToggleSpec) throws Exception {
        String baseUrl = base.replaceAll("/+$", "");
        String countUrl = countPath == null || countPath.isBlank() ? null : baseUrl + countPath;
        // 为什么要有 --reset 显式参数：三个分层端点（裸 DB / 同步 / 异步）的复位地址和参数名
        // 各不相同（activityId vs resourceId），靠 "/count" 字符串替换去猜只在探针上成立。
        // 显式传入之后，分层对照才能做到「每档都从满额度开始」——否则第 2 档起全是 2001，
        // 测到的是「名额已满有多快」，而不是抢名额有多快。
        String resetUrl;
        if (resetPath != null && !resetPath.isBlank()) {
            resetUrl = resetPath.startsWith("http") ? resetPath : baseUrl + resetPath;
        } else {
            resetUrl = countUrl == null ? null : countUrl.replace("/count", "/reset");
        }
        // 异步链路每档复位之前必须先等落库追平：复位只抹 Redis，队列里的消息稍后仍然
        // 会 UPDATE remain — 1，两边一错位，后面每一档的判据都从"账不平"开始。
        //
        // <p>这个参数存在的原因是<b>进程边界</b>：如果把「压一档」和「复位」拆成两次 java 调用，
        // 每一档都会是一次冷启动 —— 客户端要重建 TCP 连接、重新查 topic 路由。
        // 实测 800 并发下这一下能把入口吞吐压掉十倍（314 vs 3701 req/s），
        // 因为 800 个线程同时触发路由发现，全部排队等同一个查询。
        // 所以整条曲线必须在一个 JVM 里跑完，档间的 drain/reset 也只能由压测器自己发。
        String drainUrl = drainPath == null || drainPath.isBlank() ? null
                : (drainPath.startsWith("http") ? drainPath : baseUrl + drainPath);

        System.out.println();
        System.out.println("=== sweep · " + groupName + " ===");
        System.out.println("target   " + url);
        System.out.println("window   " + windowSec + "s per step, closed loop (no think time)");
        System.out.println("steps    " + sweepSpec);
        if (resetUrl != null) {
            System.out.println("reset    " + resetUrl);
        }
        if (drainUrl != null) {
            System.out.println("drain    " + drainUrl + "  (每档复位前先等落库追平)");
        }
        if (countUrl == null) {
            System.out.println("hit      (未提供 --count，跳过服务端命中数校验)");
        }
        System.out.println();
        System.out.println("conc,thr,p50,p95,p99,avg,ok,full,other,err,limited,fail%,failAll%,hit,inflightAvg,inflightPeak,idem");

        int bestConc = 0;
        long bestThr = 0;
        int lastConc = 0;
        long lastThr = 0;
        List<long[]> rows = new ArrayList<>();
        // --step-toggle：与 sweep 档位一一对应的"每档开跑前 POST 一次"的 URL 列表。
        // 存在的理由是 A/B 判决：服务端执行路径要在同一个 JVM 里交替（ABAB），
        // 否则每次重启换来的冷启动差异（实测系数约 0.62）会把要测的效应整个吃掉。
        // 输出 "#step i conc=.. toggle=.." 行给外层脚本：脚本在看到这一行时做
        // CONFIG RESETSTAT / INFO cpu 快照，从而拿到"每臂一份"的 Redis 侧数据。
        String[] toggles = stepToggleSpec == null || stepToggleSpec.isBlank()
                ? new String[0] : stepToggleSpec.split(",", -1);
        int stepIndex = -1;
        for (String token : sweepSpec.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            stepIndex++;
            int concurrency = Integer.parseInt(trimmed);
            String toggle = stepIndex < toggles.length ? toggles[stepIndex].trim() : "";
            if (!toggle.isEmpty()) {
                post(client, toggle.startsWith("http") ? toggle : baseUrl + toggle);
            }
            System.out.println("#step " + stepIndex + " conc=" + concurrency
                    + " toggle=" + (toggle.isEmpty() ? "-" : toggle));
            // 给外层脚本留出做 CONFIG RESETSTAT / 开关校验的窗口：统计是整臂口径，
            // 晚 200ms 复位就会把上一臂的尾巴算进来（首轮实测 evalCalls 因此多出 4.6k）。
            System.out.flush();
            Thread.sleep(800);

            if (drainUrl != null) {
                // 超时必须给足：drain 是长轮询，等的是消费侧追平积压。
                // 用默认的 10 秒会让它静默超时 —— 返回空串 → 判成"没追平"→ 复位被守卫拒绝
                // （9001）→ 压测继续跑在未复位的账上。实测后果是 D3 四档的记录累加成 25247 条，
                // 而每档的 ok 只有 5~8 千，账面上看不出任何异常。
                String drained = get(client, drainUrl, 300);
                if (!drained.contains("\"drained\":true")) {
                    System.out.println("# 警告: 上一档未追平就复位，本档账本会脏 → " + drained);
                }
            }
            if (resetUrl != null) {
                post(client, resetUrl);
            }
            StepStats st = runStep(client, url, idParam, targetId, pingMode, concurrency, windowSec);
            if (countUrl != null) {
                String hits = get(client, countUrl);
                try {
                    st.serverHits = Integer.parseInt(hits.trim());
                } catch (NumberFormatException ignored) {
                    st.serverHits = -1;
                }
            }

            long thr = st.wallMs == 0 ? 0 : st.total() * 1000L / st.wallMs;
            long[] sorted = st.rts.stream().mapToLong(Long::longValue).sorted().toArray();
            long p50 = sorted.length == 0 ? 0 : pct(sorted, 50);
            long p95 = sorted.length == 0 ? 0 : pct(sorted, 95);
            long p99 = sorted.length == 0 ? 0 : pct(sorted, 99);
            String hitText = st.serverHits < 0 ? "-" : st.serverHits + "/" + st.total();
            System.out.println(concurrency + "," + thr + "," + p50 + "," + p95 + "," + p99 + "," + avgRt(st) + ","
                    + st.ok.get() + "," + st.full.get() + "," + st.other.get() + "," + st.err.get() + ","
                    + st.limited.get() + "," + failPct(st) + "," + failPctAll(st) + "," + hitText + ","
                    + st.inflightAvg + "," + st.inflightPeakMax + "," + st.idem.get());
            if (st.serverHits >= 0 && st.serverHits != st.total()) {
                System.out.println("# 警告: 服务端命中 " + st.serverHits + " != 客户端 total " + st.total()
                        + " —— 这一档的吞吐是被低估的，不能当上限");
            }
            if (st.firstBad.get() != null) {
                System.out.println("# 首个非预期响应: " + st.firstBad.get());
            }
            if (thr > bestThr) {
                bestThr = thr;
                bestConc = concurrency;
            }
            lastConc = concurrency;
            lastThr = thr;
            rows.add(new long[]{concurrency, thr, p99});
        }

        System.out.println();
        System.out.println("peak  " + bestThr + " req/s @ concurrency " + bestConc);
        reportKnee(rows, bestConc, bestThr, lastConc, lastThr);
        System.out.println();
    }

    // ==================================================================================
    // 开环（固定 QPS）
    // ==================================================================================

    /**
     * 开环阶跃：按固定速率投放，不看上一请求有没有回来。
     *
     * <p><b>为什么必须做这一组</b>：闭合环里 QPS = 并发 ÷ RT，服务端一慢，客户端自然少发，
     * 于是失败率恒等于 0，曲线看上去「很稳」——那只是压力没真加上去。
     * NFR-2（失败率 &lt; 0.5%）要的是「超载时还剩下多少成功率」，
     * 只有开环能回答：速率是输入，服务端跟不上就表现为排队、超时、拒绝。
     *
     * <p><b>节拍器为什么不能阻塞投递</b>：一旦队列满就让节拍器等待，它立刻退化成闭合环——
     * 压力随服务端速度自适应，失败率又回到 0。所以队列有界、满了记 {@code rej} 并继续走。
     * {@code rej} 本身就是过载证据，不是压测器的缺陷。
     *
     * <p><b>节拍器为什么要「批量补投」</b>：Windows 上 {@code parkNanos} 的实际分辨率在毫秒级，
     * 8000 QPS 意味着每 125μs 一个 tick，逐 tick 睡会累积严重落后（实测每秒少发三成）。
     * 改为「按已流逝时间算出应该发到第几个 tick，一次补齐」，睡过头会在下一轮补回来，
     * 平均速率仍然准确。代价是补齐的那一瞬间是突发，这一点在报告里要写明。
     */
    private static void runOpenLoop(HttpClient client, String base, String url, String idParam,
                                    long targetId, boolean pingMode, String groupName,
                                    String qpsSpec, long windowSec, int poolSize,
                                    String resetPath, String drainPath) throws Exception {
        String baseUrl = base.replaceAll("/+$", "");
        String resetUrl = resetPath == null || resetPath.isBlank() ? null
                : (resetPath.startsWith("http") ? resetPath : baseUrl + resetPath);
        String drainUrl = drainPath == null || drainPath.isBlank() ? null
                : (drainPath.startsWith("http") ? drainPath : baseUrl + drainPath);

        System.out.println();
        System.out.println("=== open loop · " + groupName + " ===");
        System.out.println("target   " + url);
        System.out.println("window   " + windowSec + "s per step, OPEN loop (rate is the input)");
        System.out.println("steps    " + qpsSpec + " req/s");
        System.out.println("pool     " + poolSize + " threads, bounded queue, reject instead of blocking the pacer");
        if (resetUrl != null) {
            System.out.println("reset    " + resetUrl);
        }
        System.out.println();
        System.out.println("qps,act,thr,thrReal,finMs,p50,p95,p99,avg,ok,full,other,err,limited,rej,inflight,fail%,failAll%");

        int firstQps = 0;
        long firstThr = 0;
        int lastQps = 0;
        long lastThr = 0;
        boolean first = true;
        for (String token : qpsSpec.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int qps = Integer.parseInt(trimmed);
            // 开环下档间同样要先 drain 再复位：每档 5 秒 × 8000/s 会灌进 4 万条，
            // 不复位会让后一档直接撞上"名额已满"，测到的就不是可用性边界了。
            if (drainUrl != null) {
                String drained = get(client, drainUrl, 600);
                if (!drained.contains("\"drained\":true")) {
                    System.out.println("# 警告: 上一档未追平就复位，本档账本会脏 → " + drained);
                }
            }
            if (resetUrl != null) {
                post(client, resetUrl);
            }
            StepStats st = runOpenStep(client, url, idParam, targetId, pingMode, qps, windowSec, poolSize);

            long[] sorted = st.rts.stream().mapToLong(Long::longValue).sorted().toArray();
            long p50 = sorted.length == 0 ? 0 : pct(sorted, 50);
            long p95 = sorted.length == 0 ? 0 : pct(sorted, 95);
            long p99 = sorted.length == 0 ? 0 : pct(sorted, 99);
            long act = st.wallMs == 0 ? 0 : st.offered * 1000L / st.wallMs;
            long thr = st.wallMs == 0 ? 0 : st.total() * 1000L / st.wallMs;

            System.out.println(qps + "," + act + "," + thr + "," + thrReal(st) + "," + st.lastFinishMs + ","
                    + p50 + "," + p95 + "," + p99 + "," + avgRt(st) + ","
                    + st.ok.get() + "," + st.full.get() + "," + st.other.get() + "," + st.err.get() + ","
                    + st.limited.get() + "," + st.rejected + "," + st.inflight + ","
                    + failPct(st) + "," + failPctAll(st));
            if (st.firstBad.get() != null) {
                System.out.println("# 首个非预期响应: " + st.firstBad.get());
            }
            if (first) {
                firstQps = qps;
                firstThr = thr;
                first = false;
            }
            lastQps = qps;
            lastThr = thr;
        }

        System.out.println();
        System.out.println("note  act/thr/thrReal 是每秒速率；ok/full/other/err/rej 是本档总数 —— 两列量纲不同，别直接相除");
        System.out.println("note  act = 节拍器实际发起速率，thr = 窗口内完成速率。thr 明显小于 act 就是在堆积，不是「压测器发不出去」");
        System.out.println("note  thrReal = total / finMs（线程池排空耗时）。thr 与它差得远，说明有请求在窗口结束后才回来，"
                + "这一档的 thr 是注入速率不是吞吐；finMs=-1 表示 60 秒内没排空，只能给下界");
        System.out.println("note  rej = 客户端队列满被拒（压力真的灌满了）；inflight = 窗口结束仍在途");
        System.out.println("note  limited = 服务端 429 限流（D6），既不算成功也不算失败。"
                + "它与 rej 是两回事：limited 说明闸门在工作，rej 说明连闸门都没排上队");
        System.out.println("note  fail% = err/total（只算已发出的）；failAll% = (err+rej)/(total+rej)（含客户端丢弃）。"
                + "闭合环下 rej=0，两者应相等 —— 不等就是统计算错了");
        if (firstQps > 0) {
            System.out.println("note  首档 " + firstQps + " → 末档 " + lastQps + " req/s：完成速率 "
                    + firstThr + " → " + lastThr + " req/s；"
                    + "若末档 thr < act 或 fail% 跳升，可用性边界就在这两档之间");
        }
        System.out.println();
    }

    /**
     * 压一档开环，返回统计。
     *
     * <p>窗口的起止点是「节拍器开始投放」到「计划的最后一个 tick 到期」，
     * 之后的在途请求最多再等 {@code drainSec} 秒——它们的 RT 已经算进样本（那是真实用户体验），
     * 但不计入窗口吞吐，否则超时请求会把分子做大、把窗口拉长，吞吐反而虚高。
     */
    private static StepStats runOpenStep(HttpClient client, String url, String idParam, long targetId,
                                         boolean pingMode, int targetQps, long windowSec,
                                         int poolSize) throws Exception {
        StepStats st = new StepStats();
        boolean probeMode = url.contains("/lua/");
        // 队列容量与目标速率挂钩：太小会让 rej 在任何速率下都非零（测到的是队列，不是服务端），
        // 太大会让「积压」滞后好几秒才显现，还可能把堆吃满。
        int queueCap = Math.max(2048, targetQps * 2);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                poolSize, poolSize, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCap),
                r -> {
                    Thread t = new Thread(r, "open-worker");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());
        pool.prestartAllCoreThreads();

        long startNanos = System.nanoTime();
        long intervalNanos = Math.max(1L, 1_000_000_000L / targetQps);
        long endNanos = startNanos + windowSec * 1_000_000_000L;
        long ticks = 0;
        long rejected = 0;

        while (true) {
            long now = System.nanoTime();
            if (now >= endNanos) {
                break;
            }
            long dueTicks = (now - startNanos) / intervalNanos;
            while (ticks < dueTicks) {
                if (startNanos + ticks * intervalNanos >= endNanos) {
                    break;
                }
                final String fullUrl = pingMode ? url
                        : url + (url.contains("?") ? "&" : "?")
                        + idParam + "=" + targetId + "&studentId=S" + ticks + "_" + System.nanoTime();
                final long enqueueNanos = System.nanoTime();
                ticks++;
                try {
                    pool.execute(() -> openCall(client, fullUrl, pingMode, probeMode, st, enqueueNanos));
                } catch (RejectedExecutionException e) {
                    rejected++;
                }
            }
            // 最多睡 2ms：睡太久会让补齐的突变更大，睡太勤会空转抢 CPU。
            LockSupport.parkNanos(Math.min(intervalNanos, 2_000_000L));
        }

        long wallMs = (System.nanoTime() - startNanos) / 1_000_000;
        pool.shutdown();
        boolean done = pool.awaitTermination(60, TimeUnit.SECONDS);
        // 排空时刻必须在 awaitTermination 之后取：它包含窗口结束后仍在跑的尾巴，
        // 而 wallMs 在 shutdown 前就定格了。两者之差就是「窗口外才完成」的那段时间。
        long finishMs = (System.nanoTime() - startNanos) / 1_000_000;
        st.wallMs = wallMs;
        st.offered = ticks;
        st.rejected = rejected;
        st.inflight = done ? 0 : Math.max(0, pool.getTaskCount() - pool.getCompletedTaskCount());
        st.lastFinishMs = done ? finishMs : -1;
        return st;
    }

    /** 开环的一次请求。RT 从「进入队列」算起，包含排队时间——那才是用户实际等待的时长。 */
    private static void openCall(HttpClient client, String fullUrl, boolean pingMode, boolean probeMode,
                                 StepStats st, long enqueueNanos) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(fullUrl))
                    .timeout(Duration.ofSeconds(10));
            HttpRequest req = pingMode || probeMode
                    ? builder.GET().build()
                    : builder.POST(HttpRequest.BodyPublishers.noBody()).build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            recordRt(st, enqueueNanos);
            String body = resp.body() == null ? "" : resp.body();
            if (pingMode) {
                if (resp.statusCode() == 200) {
                    st.ok.incrementAndGet();
                } else {
                    st.other.incrementAndGet();
                    recordBad(st, resp.statusCode(), body);
                }
            } else if (probeMode) {
                if (body.startsWith("ok:")) {
                    st.ok.incrementAndGet();
                } else {
                    st.other.incrementAndGet();
                    recordBad(st, resp.statusCode(), body);
                }
            } else if (resp.statusCode() == 429) {
                st.limited.incrementAndGet();
            } else if (body.contains("\"code\":0")) {
                st.ok.incrementAndGet();
            } else if (body.contains("\"code\":2001")) {
                st.full.incrementAndGet();
            } else {
                st.other.incrementAndGet();
                recordBad(st, resp.statusCode(), body);
            }
        } catch (Exception e) {
            recordRt(st, enqueueNanos);
            st.err.incrementAndGet();
            st.firstBad.compareAndSet(null, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * 判「拐点」，而不是判「峰值出现在哪」。
     *
     * <p>这两种形状完全不同，但在"最后一档吞吐最高"的曲线上长得一模一样：
     * <pre>
     *   过载：吞吐冲顶后回落      100→5000, 200→6000, 400→5800, 800→5200
     *   饱和：吞吐持平、延迟线性涨 100→2600, 200→3900, 400→4366, 800→4435   ← D5 实测的 mq 曲线
     * </pre>
     * 后者峰值恰好在最后一档，若只按「峰值 == 最后一档」判定，就会报"还没扫到拐点"，
     * 而真相是它早在 400 并发就已经满了 —— 再往上加的那 400 个线程，只是在排队。
     *
     * <p>饱和的判据不用绝对值，用两个比值：吞吐涨幅 &lt; 5% 而 p99 涨幅 &gt; 50%。
     * 加了一倍并发，吞吐只多 5% 却把排队时间翻了一番，说明新增的并发全落在队列里。
     */
    private static void reportKnee(List<long[]> rows, int bestConc, long bestThr,
                                   int lastConc, long lastThr) {
        if (rows.size() < 2) {
            System.out.println("note  只有一档，判不了拐点");
            return;
        }
        // 「达到峰值 95% 的最小并发」是最有信息量的一个数：
        // 它告诉你为了拿到这个容量，最少需要多少并发，再往上都是白加。
        int kneeConc = lastConc;
        for (long[] r : rows) {
            if (r[1] * 100 >= bestThr * 95) {
                kneeConc = (int) r[0];
                break;
            }
        }
        System.out.println("knee  concurrency " + kneeConc
                + " 起达到峰值 95%（再多加并发只涨排队时间，不涨吞吐）");

        long[] last = rows.get(rows.size() - 1);
        long[] prev = rows.get(rows.size() - 2);
        double thrGain = prev[1] == 0 ? 0 : (last[1] - prev[1]) * 100.0 / prev[1];
        double p99Gain = prev[2] == 0 ? 0 : (last[2] - prev[2]) * 100.0 / prev[2];

        if (lastConc != bestConc) {
            System.out.println("note  峰值之后已经回落（最后一档 " + lastConc + " 并发 = " + lastThr
                    + " req/s），过载拐点就在 " + bestConc + " 附近");
        } else if (thrGain < 5.0 && p99Gain > 50.0) {
            System.out.printf("note  %d→%d 并发：吞吐 %+.1f%% 而 p99 %+.0f%% —— 已饱和。"
                    + "饱和点在前一档 %d，这一档多出来的线程全在排队%n",
                    prev[0], last[0], thrGain, p99Gain, prev[0]);
        } else {
            System.out.println("note  最高吞吐出现在最后一档，且吞吐仍在涨（"
                    + String.format("%+.1f%%", thrGain) + "）—— 这一组还没扫到拐点，"
                    + "得继续加并发；如果加到这个数还在涨，就该怀疑瓶颈不在被测的这一层");
        }
    }

    /**
     * 压一档，返回这一档的统计。
     *
     * <p>窗口起点取「所有线程放行的那一刻」，而不是「线程创建之前」：
     * 800 个线程的创建本身要几十到几百毫秒，算进窗口就等于给每一档都掺了一段空窗，
     * 并发越高掺得越多，曲线会被人为压平。
     */
    private static StepStats runStep(HttpClient client, String url, String idParam, long targetId,
                                     boolean pingMode, int concurrency, long windowSec) throws Exception {
        StepStats st = new StepStats();
        // 探针返回的是 "ok:N" 纯文本，不是 JSON 信封，判定方式必须分开。
        // 混用的话探针的每一个成功都会被记进 other，吞吐看着"正常"而成功率是 0。
        boolean probeMode = url.contains("/lua/");
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        AtomicLong deadline = new AtomicLong();
        /** 在途请求数：进入请求前 +1，判定完 -1。采样线程按时间积分它得到平均在途数。 */
        AtomicInteger inflight = new AtomicInteger();

        for (int t = 0; t < concurrency; t++) {
            final int tid = t;
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                    int i = 0;
                    while (System.nanoTime() < deadline.get()) {
                        String sid = "S" + tid + "_" + i++ + "_" + System.nanoTime();
                        String fullUrl = pingMode ? url
                                : url + (url.contains("?") ? "&" : "?")
                                + idParam + "=" + targetId + "&studentId=" + sid;
                        inflight.incrementAndGet();
                        long t0 = System.nanoTime();
                        try {
                            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(fullUrl))
                                    .timeout(Duration.ofSeconds(10));
                            // 探针是 GET：它是"读一次 Redis"，语义上不是写入。
                            // 用 POST 打过去会 405，而 405 的响应体不含 code:0 → 全部记成 other。
                            HttpRequest req = pingMode || probeMode
                                    ? builder.GET().build()
                                    : builder.POST(HttpRequest.BodyPublishers.noBody()).build();
                            HttpResponse<String> resp = client.send(req,
                                    HttpResponse.BodyHandlers.ofString());
                            recordRt(st, t0);
                            String body = resp.body() == null ? "" : resp.body();
                            if (pingMode) {
                                if (resp.statusCode() == 200) {
                                    st.ok.incrementAndGet();
                                } else {
                                    st.other.incrementAndGet();
                                    recordBad(st, resp.statusCode(), body);
                                }
                            } else if (probeMode) {
                                // 探针返回 "ok:{code}:{remain}"。code 是 claim.lua 的出口码：
                                // 0 成功 / -1 名额满 / -2 幂等命中 / -3 库存 key 丢失 / -4 学院满。
                                // 只看前缀 "ok:" 会把 -1 也算成成功 —— D7 之前所有探针数据都是这个口径，
                                // 于是"ok 数"其实是"脚本正常执行数"，不能当"抢到名额数"用。
                                int code = probeCode(body);
                                if (code == 0) {
                                    st.ok.incrementAndGet();
                                } else if (code == -1) {
                                    st.full.incrementAndGet();
                                } else if (code == -2) {
                                    st.idem.incrementAndGet();
                                } else {
                                    st.other.incrementAndGet();
                                    recordBad(st, resp.statusCode(), body);
                                }
                            } else if (resp.statusCode() == 429) {
                                // D6：入口限流的主动拒绝。既不算成功也不算故障，
                                // 单独计数才能算出「放行精度」＝ 实际放行 ÷ 设定速率。
                                st.limited.incrementAndGet();
                            } else if (body.contains("\"code\":0")) {
                                st.ok.incrementAndGet();
                            } else if (body.contains("\"code\":2001")) {
                                st.full.incrementAndGet();
                            } else {
                                st.other.incrementAndGet();
                                recordBad(st, resp.statusCode(), body);
                            }
                        } catch (Exception e) {
                            recordRt(st, t0);
                            st.err.incrementAndGet();
                            st.firstBad.compareAndSet(null, e.getClass().getSimpleName() + ": " + e.getMessage());
                        } finally {
                            // 放在 finally 而不是两条分支各写一次：判定块有 5 个出口，
                            // 漏一个就会让在途数只增不减，采样出来的均值一路爬升。
                            inflight.decrementAndGet();
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "sweep-" + t);
            worker.setDaemon(true);
            worker.start();
        }

        long releaseNanos = System.nanoTime();
        deadline.set(releaseNanos + windowSec * 1_000_000_000L);
        start.countDown();

        // 采样从窗口真正开始的那一刻起，而不是更早：提前启动会把「还没发请求」的 0 也平均进去。
        // 采样间隔 5ms（Windows 上实际落在 15.6ms 的时钟粒度上，5 秒窗口约 300 个样本，
        // 对平均值足够；峰值会略低，因为它只能看见采样点上的瞬间）。
        AtomicLong samples = new AtomicLong();
        AtomicLong sampleSum = new AtomicLong();
        AtomicInteger samplePeak = new AtomicInteger();
        Thread sampler = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                int cur = inflight.get();
                sampleSum.addAndGet(cur);
                samples.incrementAndGet();
                samplePeak.accumulateAndGet(cur, Math::max);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "sweep-inflight");
        sampler.setDaemon(true);
        sampler.start();

        done.await();
        sampler.interrupt();
        st.inflightAvg = samples.get() == 0 ? 0 : sampleSum.get() / samples.get();
        st.inflightPeakMax = samplePeak.get();
        st.wallMs = (System.nanoTime() - releaseNanos) / 1_000_000;
        return st;
    }

    /** 同步 GET，只要响应体；失败返回空串（探针/计数接口挂了不该让整轮扫描崩掉）。 */
    private static String get(HttpClient client, String url) {
        return get(client, url, 10);
    }

    /**
     * 同步 GET，可指定超时。
     *
     * <p>{@code /api/claim/dev/drain} 是长轮询：它要等消费侧把积压追平才返回，
     * 而积压 2 秒的消息在 330 条/秒的消费速度下要 20 秒以上。用 10 秒的默认值调用它，
     * 超时不会报错，只会返回空串 —— 调用方于是以为"没追平"，静默地把后面的复位也放弃掉。
     * 这类失败在日志里只留一行警告，很容易被当成噪音。
     */
    private static String get(HttpClient client, String url, int timeoutSec) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSec))
                    .GET()
                    .build();
            return client.send(req, HttpResponse.BodyHandlers.ofString()).body();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 同步 POST，返回响应体（失败返回空串）。复位接口用。
     *
     * <p>为什么复位必须用 POST 而不是 GET：{@code /api/claim/dev/reset} 和
     * {@code /api/baseline/reset} 都是 {@code @PostMapping}，用 GET 打过去是 405。
     * 405 的响应体不含 {@code "code":0}，但复位结果从来没人检查 —— 于是会静默地
     * 「每档都以为复位过了，其实名额在第一档就打光了」，第 2 档起测的全是 2001。
     */
    private static String post(HttpClient client, String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            String body = client.send(req, HttpResponse.BodyHandlers.ofString()).body();
            if (body != null && !body.contains("\"code\":0")) {
                System.out.println("# 警告: 复位失败 " + url + " → " + body);
            }
            return body == null ? "" : body;
        } catch (Exception e) {
            System.out.println("# 警告: 复位请求异常 " + url + " → " + e.getMessage());
            return "";
        }
    }

    /**
     * 解析探针响应 {@code "ok:{code}:{remain}"} 里的 code，返回 {@link Integer#MIN_VALUE} 表示不认识。
     *
     * <p>为什么要把 code 从响应体里抠出来而不是信 HTTP 状态码：探针刻意把业务码也回 200
     * （500 会被压测器记成"Redis 挂了"，而实际可能只是名额满了），所以状态码永远是 200，
     * 唯一的区分就是这一小段文本。
     */
    private static int probeCode(String body) {
        if (body == null || !body.startsWith("ok:")) {
            return Integer.MIN_VALUE;
        }
        int end = body.indexOf(':', 3);
        String num = end < 0 ? body.substring(3) : body.substring(3, end);
        try {
            return Integer.parseInt(num.trim());
        } catch (NumberFormatException e) {
            return Integer.MIN_VALUE;
        }
    }

    private static long pct(long[] sorted, int p) {
        int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
