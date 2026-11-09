# B4 · MQ 不可用的真实形态（2026-11-09 16:14）

## 做法

`r1 = 1000000`（**没有加配额封顶**，理由见文末），100 并发闭合环压 60 秒，
在 **T+4s** 用 `mqshutdown broker` 发 SIGTERM：

```
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 100 1 1 /api/claim/mq resourceId \
     --sweep=100 --seconds=60
```

## 时间线（`logs/b4-timeline.txt`）

| 时刻 | 事件 |
|---|---|
| 16:14:31 | T0，压测开始 |
| 16:14:39 | `mqshutdown broker` → SIGTERM 到 broker JVM（PID 24） |
| 16:14:44 | 容器退出（5 秒），**restart policy 自动拉起**，不需要 `docker start` |
| 16:15:46 | `clusterList` 重新出现 `broker-a`（`#ACTIVATED true`） |

**SIGTERM → broker-a 可见 = 67 秒。**（第一次试跑测到 76 秒，两次同量级）

对照 11-05 的 `docker stop`：10 分钟。机制在 `cs-broker` 的 PID1 上——
PID1 是 `sh mqbroker`，**shell 不转发信号**，SIGTERM 到不了 JVM，10 秒宽限后直接 SIGKILL，
于是下次启动走异常恢复（重建 ConsumeQueue）。`mqshutdown` 的脚本只是
`ps ax | grep BrokerStartup | awk '{print $1}'` 然后 `kill` 那个 **java PID**，
所以是 JVM 自己 flush 后退出。同一个"停"，走了两条完全不同的路径。

## 压测结果（60 秒窗口）

| 指标 | 值 |
|---|---|
| 总请求 | 149,793 |
| 成功 | **33,158**（22%，集中在前 ~4 秒） |
| `9003 MQ_UNAVAILABLE` | **116,635**（78%） |
| err / 超时 | 0 |
| 平均吞吐 | 2,495 req/s |
| p50 / p95 / p99 | **33ms / 49ms / 471ms** |
| 平均 RT | 39ms |

## 结论（和预期相反，这是这次最值钱的数字）

**没有出现"每请求阻塞 3 秒、吞吐塌到 66/s"。**

`socketReadTimeout`/`setRequestTimeout(3000)` 只在「连接还在、对端不回包」时生效。
broker 进程死了之后，TCP 连接被 RST/FIN 掉，`producer.send()` **立刻抛错**（~40ms），
不走 3 秒等待。所以：

- 停机的代价是 **78% 的请求被拒**，不是**吞吐塌陷**，也不是**线程被占满**；
- 吞吐 2,495 req/s 反而**高于** B1 同并发（100）那一档的 2,342 —— 因为 9003 分支比成功分支少一次 MQ 投递；
- 「异步化把 MQ 变成了同步依赖点」这句话仍然成立，但它的失效形态是**快速失败**，
  不是排队阻塞。3 秒超时要等"broker 卡住但没死"（网络丢包 / GC 停顿 / 磁盘 hang）才会出现，
  那是 B5 和 B7 的边界，不是 B4 的。

## 回补是对的：一份配额都没丢

失败分支走 `compensate()`（INCR stock + DEL 幂等键，`AsyncClaimService:216-220`），
所以停机窗口内不产生记录、不消耗配额。实测：

```
redis 已消耗 33,158  = ok 33,158            ← 失败请求一个都没留在 stock 上
records 6,504 + pending 26,654 = 33,158     ← 消息一条没少，只是还在队列里
idemKeys 33,158                             ← 与成功数一一对应
drift −26,654 = −pending                    ← 只是积压，不是丢账
```

`pending` 排干后 `drift` 归 0 才算这条通过（见下一节）。

## 两个操作细节

1. **表里的 `reset quota=20000` 和"去掉配额封顶"是冲突的**：封顶 20000 之后，
   前 3 秒就把额度打光，剩下 57 秒全是 `2001`（**不调 send()**），
   于是"停机窗口的发送失败"一次都测不到，得到的会是 2001 路径的吞吐。本次用 100 万跑。
2. **`MQ_UNAVAILABLE` 是 `9003`，不是 `1601`**（`ErrorCode:53`）。报告里写 1601 会和日志对不上。
