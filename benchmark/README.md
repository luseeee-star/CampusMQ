# benchmark · 压测与实验资产

> **这个目录的权重被严重低估**：代码是同质化的，数据是稀缺的。
> D13 那份报告的全部数字都出自这里，报告写不好通常不是文笔问题，是这里没有数据。

## 已就绪

| 文件 | 落地日 | 作用 |
|---|---|---|
| `hammer/BaselineHammer.java` | D1 ✅ / D2 ✅ / D5 ✅ | 抢名额压测器。**同一个程序跑四组对照**，只改 path 与模式参数 |
| `d5-sweep.ps1` | D5 ✅ | 三条曲线（ping / 单 key Lua / 全链路）一次跑完，出拐点与饱和判定 |
| `fsync-ab.ps1` | D5 ✅ | 消费侧 323 条/秒的归因 A/B：只改 MySQL 两个刷盘开关，量消费速率与等锁线程数 |
| `layered.ps1` | D5 ✅ | 三层防线对照（§8.2）+ 失败率（NFR-2）+ Redis commandstats |
| `open-loop.ps1` | D5 ✅ | 开环（固定 QPS）测可用性边界（NFR-6）：8000 QPS 是"收得下"还是"来不及" |
| `q7-steady.ps1` | 2026-11-10 ✅ | 开环两档线程（768/2048）裁决 `3,845` 到底是系统的墙还是压测器的墙 |
| `d7r-closedloop.ps1` | 2026-11-10 ✅ | 闭环 2×2（冷/热 × consumer 开/停）**只翻 consumer 一个开关**，归因 9,256 与 10,650 那 13%；`-Arms B,A -Tag 2 -Append` 用来跑反序第二轮 |
| `night-smoke.ps1` | 2026-11-10 ✅ | 一晚改完后的三项判据一次跑完（一次 app 生命周期、12 条）：超长学号 1001 且 stock 不动、single-flight 每个失效窗口只回源 1 次、`close_at` 到点自动 CLOSED + 出报表。**只建临时活动，不碰 r1/r3** |

JDK 21 单文件源码模式，零依赖零构建：

```bash
# 固定次数模式（D1/D2 用；测的是"斜坡平均值"，不适合判吞吐上限）
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3
#                                                        ↑base ↑并发  ↑每线程次数

# 阶跃加压模式（闭合环，按时长）：每档 N 秒，出 p50/p95/p99 与拐点判定
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 0 0 9001 /api/dev/ping none \
     --sweep=100,200,400,800 --seconds=5 --count=/api/dev/ping/count

# 开环模式（固定 QPS，QPS 是输入不是结果）：测超载时的失败率与排队
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 0 0 1 /api/claim/mq resourceId \
     --qps=2000,4000,6000,8000 --seconds=5 --threads=768 \
     --reset='/api/claim/dev/reset?resourceId=1&quota=500000' \
     --drain='/api/claim/dev/drain?resourceId=1&timeoutMs=600000'
```

**`--reset` / `--drain` 存在的唯一理由是「整条曲线必须在一个 JVM 里跑完」。**
把「压一档」拆成一次独立进程调用，每一档都会是一次冷启动：客户端重建连接、重做 topic 路由发现，
800 个线程同时触发同一次查询。实测这一项能把 D3 的 800 并发档从 3701 打到 **314 req/s**，
而曲线形状看起来完全像过载。**这不是性能差别，是数据有效性差别。**
所以档间的 drain / reset 只能由压测器自己发，不能回到 PowerShell 循环里。

**两种模式测的不是同一个东西。** 固定次数模式下「200 并发 × 30 次」= 6000 个请求，
跑完只花 1.3 秒，其中大半是建连与 JIT —— 那个数字是斜坡平均值。
实测同一个 `/api/dev/ping`：固定次数 4.5k、按时长闭合环 45.6k，**差十倍**。
判容量一律用 `--sweep`，固定次数只用来验正确性。

D5 的完整一轮（含预热、两轮取样、账目校验）：

```bash
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
    -Work "& ./benchmark/d5-sweep.ps1"
```

消费速度的归因 A/B（约 3 分钟，跑完自动改回 `flush=1 / sync_binlog=1`）：
```bash
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
    -Work "& ./benchmark/fsync-ab.ps1"
```

**它测的是「消费侧的天花板由什么决定」，不是「调参能快多少」。** 两个 arm 只差 MySQL 的
`innodb_flush_log_at_trx_commit` 与 `sync_binlog`，其余一字不改。结论用来给 D10 排序：
若 fsync 是主因就先批量提交，若行锁是主因就先拆热点。实测 319→1098（3.4x）且 fast arm
里仍有 12.7/20 线程在等锁 —— 两道墙都存在。数据见 LOGBOOK D5 第 4 节。

三层防线对照（§8.2 + NFR-2 + Redis 占用，约 4 分钟，其中 2 分钟在等 D3 落库）：

```bash
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
    -Work "& ./benchmark/layered.ps1"
```

它回答的是**另一个**问题，别和 `d5-sweep.ps1` 混：

- `d5-sweep.ps1`：同一个 JVM 里逐层加组件（ping → +Redis → +MQ），答「时间花在哪一层」
- `layered.ps1`：三个不同实现打同一个语义接口（裸 DB / 同步 / 异步），答「每层防线挡住多少」

脚本分两段，缺一不可。段 A 的 quota 给到 20 万，保证一档都耗不尽（否则第 2 档起测的是
「拒绝一个请求有多快」）；段 B 的 quota 故意给 500，只有请求数真的超过名额，
「会不会超卖」这个问题才被问到 —— 实测 D1 超 62 件、D2/D3 精确 500。
**把这两段合成一段，两边的结论都会消失。**

开环可用性边界（NFR-6，约 6 分钟，高 QPS 档要等落库追平）：

```bash
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
    -Work "& ./benchmark/open-loop.ps1"
```

两组：先 ping 标定客户端天花板（16000 QPS 全部达成），再灌业务链路。
判据不是「吞吐够不够」，是三件事一起看：`act/qps`（节拍器跟没跟上）、
`thr/act`（在不在堆积）、`fail%`（NFR-2 口径）。
`rej` 不是压测器缺陷，是「在途请求已超过线程数+队列容量」的直接证据 —— 闭合环永远看不到它。

> **延迟在这台机器上不可复现。** 同一个 4000 QPS 档，两轮跑出 P99 = 97ms 和 1235ms，差 12 倍，
> 差别只在于前面跑了几档、broker 的 PageCache 热不热。吞吐和失败率两轮几乎一样。
> 所以：**吞吐可以给单一数字，延迟只能给区间 + 测量条件。**

> 这两个参数在 2/0 下崩溃会丢事务。脚本用 `try/finally` 恢复并回读校验，
> 但**如果进程被硬杀，必须手工改回来**：
> `SET GLOBAL innodb_flush_log_at_trx_commit=1; SET GLOBAL sync_binlog=1;`

**为什么坚持同一个压测器，而不是复制一份改改**：两份程序一旦在「线程怎么起、
起跑线怎么同步、RT 从哪一刻开始算」上有一点不一致，两轮数据就不可比，
而这种差异肉眼完全看不出来。

> D1/D2 不用 Gatling。它值钱的地方（阶跃加压、自动出 RT 曲线）要到 D5 才用得上，
> 而这两天只需要「固定并发打一个接口」。改一个数字就能重跑，比装一套压测框架便宜。

> **Git Bash 下必须 `export MSYS_NO_PATHCONV=1`**，否则 `/api/claim` 会被改写成
> `D:/.../PortableGit/api/claim`，600 个请求全部 `unsupported URI`。
> PowerShell / CMD 没有这个问题。

## 待落地

| 文件 | 落地日 | 作用 |
|---|---|---|
| `seed-data.sql` | D11 | 3.2 万学生 / 20 个资源 / 名单 |
| ~~`gatling/` 或 `*.jmx`~~ | ~~D5~~ | **D5 决定不引入**：阶跃加压已在 `BaselineHammer --sweep` 里实现，多引一个框架就多一套不可比的 RT 口径 |
| `shard-compare.md` | D7 | 分片前/后同条件对比记录 |
| `consume-test/` | D10 | 灌 100 万条，测队列数 × 消费线程的拐点 |
| `chaos.sh` | D10 | 停 Redis / 停 Broker / 卡死消费者 / 慢查询 / 磁盘近满 |

## 每轮实验的固定流程

少一步数据就废了，顺序不能换。**两组接口不同，判据也不同**：

```bash
# ---------- 对照组 1：裸 DB ----------
curl -X POST "http://127.0.0.1:8080/api/baseline/reset?activityId=1&quota=200"
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3
curl "http://127.0.0.1:8080/api/baseline/stat?activityId=1"
#    判据：remain < 0，超卖件数 = -remain

# ---------- 对照组 2：Redis Lua ----------
curl -X POST "http://127.0.0.1:8080/api/claim/dev/reset?resourceId=1&quota=200"
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3 1 /api/claim resourceId
curl "http://127.0.0.1:8080/api/claim/dev/stat?resourceId=1"
#    判据：oversold = 0 且 drift = 0，records 必须恰好等于容量
```

**两套判据不能混用。** 裸表允许 remain 为负，所以 D1 看 `remain < 0`；
主表有 `CHECK (remain >= 0)`，超卖表现为「记录数超过名额」，remain 永远不是负数。
拿 D1 的判据去看 D2，会把成功读成失败。

## 记账纪律（不可协商）

任何一组数据都必须同时记录四个数字，并且能连起来：

```text
入口请求数 → 被过滤数 → 进入 MQ 数 → 实际落库数
```

只写「QPS 从 X 降到 Y」而没有落库行数的数据，一律视为无效数据。
这正是需求书 §1 拆解的那个坑：把 `8000 QPS ÷ 1200 TPS = 7 倍` 当成「写压力」，
是拿「每秒请求数」除以「每秒事务数」，量纲根本不能除。

## 同机压测的局限（必须写进报告）

压测端与被测端在同一台机器上，CPU、内存、磁盘 IO 全在抢。

- 可以用的：**相对对照**（裸 DB 版 vs Redis 版谁快、分片前后差多少）
- 不能用的：**绝对容量**（"本系统支撑 8000 QPS" 这种结论，同机测不出来）

报告和 README 都写这一句：「本机压测，数据用于相对对照，非绝对容量。」
主动说出来比被面试官问出来强。若要走 Docker 隔离（`--cpus=2`），必须
应用与压测器分在两个容器里，否则限制的是同一份 CPU。

## 压测前置五件事

1. 数据预热：JIT 预热 2 分钟、库存初始化，否则首轮数据必然偏低
2. **用 `--spring.profiles.active=bench` 启动**，关掉 SQL 日志。
   不这么做会同时掩盖性能和竞态：实测裸版超卖从 65–86 件掉到 54–67 件，
   因为日志在**持有数据库连接期间**打印，连接周转变慢、有效并发被压低。
   两组对照如果日志配置不同，数字根本没法比。
3. 确认 `quota:*` 相关 key 没被 evict（Redis 必须是 `maxmemory-policy noeviction`）
4. **确认 6379 是自己的 Redis。** 若本机还有别的 compose 项目（`restart: always`），
   Docker 引擎一启动它就会抢占 6379，`cs-redis` 起不来但 `docker ps` 里看不到冲突原因
   （`docker port cs-redis` 为空就是端口没绑上）。跑前 `docker stop <别的 redis>`，跑完再 `docker start`。
5. **Docker 冷启动后 broker 可能永远起不来。** 现象是 `cs-proxy` 反复重启、
   `mqadmin clusterList` 返回空表头。根因是 store 里的 `abort` 文件触发异常恢复，
   而恢复路径上的日志 appender 在 Windows 挂载点上建不了文件、把 main 线程堵死。
   把 `<store>/abort` 改名备份再重启即可（不是本项目代码的问题，但每次重启 Docker 都会遇到）。
