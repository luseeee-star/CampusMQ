import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用「真实名单学号」并发打 /api/claim/mq。
 *
 * <p><b>为什么不能用 BaselineHammer</b>：它每个请求生成的学号是
 * {@code S{tid}_{i}_{nanoTime}}，在 {@code roster_required=1} 的活动上全部返回 1005，
 * 测到的是「名单判定」而不是「配额」。而 r2 的 200 个学号是写死在
 * {@code quota_roster} 里的，压测器必须照着这份名单打。
 *
 * <p><b>为什么要打乱</b>：名单按学院分段存放（2023000001~200 每 50 人一个学院）。
 * 顺序打的话，前 80 个请求全落在前两个学院，第三个学院根本压不到并发；
 * 打乱之后每个学院同时有 50 个请求争 20 个名额，才能看出双层配额在并发下会不会被穿透。
 * 种子固定，结果可复现。
 *
 * <p><b>dup 模式</b>：同一个学号连打 N 次，验的是幂等（应只成功 1 次、响应体一致），
 * 而不是配额。两种模式的判据完全不同，不能混在一次压测里。
 *
 * <pre>
 * java benchmark/RosterBurst.java http://127.0.0.1:8080 2 logs/r2_roster.txt 200
 * java benchmark/RosterBurst.java http://127.0.0.1:8080 2 logs/r2_roster.txt 1 dup 100
 * </pre>
 */
public class RosterBurst {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: RosterBurst <base> <resourceId> <sidFile> [threads] [burst|dup] [dupTimes]");
            return;
        }
        String base = args[0];
        String rid = args[1];
        String file = args[2];
        int threads = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        String mode = args.length > 4 ? args[4] : "burst";
        int dupTimes = args.length > 5 ? Integer.parseInt(args[5]) : 100;

        List<String> sids = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(file))) {
            String s = line.strip();
            if (!s.isEmpty()) {
                sids.add(s);
            }
        }
        if (sids.isEmpty()) {
            System.out.println("学号文件为空：" + file);
            return;
        }
        Collections.shuffle(sids, new Random(20261109L));

        List<String> jobs = new ArrayList<>();
        if ("dup".equals(mode)) {
            for (int i = 0; i < dupTimes; i++) {
                jobs.add(sids.get(0));
            }
        } else {
            jobs.addAll(sids);
        }

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, threads));
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(jobs.size());
        ConcurrentHashMap<String, AtomicInteger> codes = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, AtomicInteger> perSidHits = new ConcurrentHashMap<>();
        List<String> detail = Collections.synchronizedList(new ArrayList<>());

        for (String sid : jobs) {
            pool.submit(() -> {
                try {
                    start.await();
                    String url = base + "/api/claim/mq?resourceId=" + rid + "&studentId=" + sid;
                    HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                            .timeout(Duration.ofSeconds(15))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build();
                    HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
                    String body = resp.body();
                    String code = "HTTP" + resp.statusCode();
                    int idx = body.indexOf("\"code\":");
                    if (idx >= 0) {
                        int end = body.indexOf(',', idx);
                        code = body.substring(idx + 7, end).trim();
                    }
                    codes.computeIfAbsent(code, k -> new AtomicInteger()).incrementAndGet();
                    perSidHits.computeIfAbsent(sid, k -> new AtomicInteger()).incrementAndGet();
                    detail.add(sid + "," + code);
                } catch (Exception e) {
                    String key = "EXC:" + e.getClass().getSimpleName();
                    codes.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
                    detail.add(sid + ",EXC");
                } finally {
                    done.countDown();
                }
            });
        }

        long t0 = System.nanoTime();
        start.countDown();
        boolean finished = done.await(180, TimeUnit.SECONDS);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        pool.shutdownNow();

        System.out.println("mode=" + mode + " requests=" + jobs.size() + " threads=" + threads
                + " costMs=" + ms + " finished=" + finished);
        codes.entrySet().stream()
                .sorted((a, b) -> b.getValue().get() - a.getValue().get())
                .forEach(e -> System.out.println("  code " + e.getKey() + " -> " + e.getValue().get()));
        if ("burst".equals(mode)) {
            System.out.println("distinctSids=" + perSidHits.size());
        }
        Path out = Path.of("logs/roster-burst-" + mode + "-" + System.currentTimeMillis() + ".csv");
        Files.write(out, detail);
        System.out.println("detail: " + out);
    }
}
