# r2 学院层判据（2026-11-09 15:56）

补测：把 `total_quota` 抬到 200，让 stock 留富余，验证学院配额在并发下到底有没有被穿透。

## 为什么第一次 r2 的结论不成立

第一次 r2 是 `total=80`、`Σcollege=4×20=80`，两个数相等。`claim.lua` 的判定顺序是 stock 满 → 学院满：

```lua
if stock <= 0 then return {-1, 0} end        -- 2001，根本不看学院
if useCollege then ... if collegeStock <= 0 then return {-4, 0} end
```

80 个成功先把 stock 打到 0，剩下 120 个请求在 stock 判定处就返回 2001，压根没走到学院判定。
「每院精确 20」是 80 ÷ 4 的算术结果，不是并发下学院层未被穿透的证据。
当时的分布 `2001 拒 114 / 2002 拒 6` 恰恰是反证——学院层若真被压到，2002 该是 120。

## 补测配置

`total_quota / remain / stock = 200`，学院仍 `4 × 20 = 80`，故意留 120 缺口。

```
POST /api/claim/dev/reset?resourceId=2&quota=200
→ totalQuota=200 redisRemain=200 collegeRows=4 redisCollegeKeys=4
→ clearedIdemKeys=2 clearedRosterKeys=200 loadedRosterKeys=200
```

## 压测

`RosterBurst` burst 模式，200 个真实学号各打一次，200 线程，同一起跑线：

```
java benchmark/RosterBurst.java http://127.0.0.1:8080 2 logs/r2_roster.txt 200
mode=burst requests=200 threads=200 costMs=1033 finished=true
  code 0    -> 80
  code 2002 -> 120
  code 2001 -> 0
distinctSids=200
```

## 判据：三种码的分布方向反过来了

| 观测量 | 值 | 含义 |
|---|---|---|
| 2001 | 0 | 没有一个请求因「总名额满」被拒 |
| 2002 | 120 | 剩下 120 个全部死在学院判定，且 120 = 200 − 80 |
| stock | 120 | 总名额确实还剩 120，不是被 stock 挡下的 |

这是学院层判据的干净形态：总量有富余、学院层精确拦住、拒绝码落在 2002。

## 逐学院（按 `quota_roster` 的真实归属）

| college | requests | ok | 2002 | 2001 |
|---|---|---|---|---|
| ART | 50 | 20 | 30 | 0 |
| CS | 50 | 20 | 30 | 0 |
| EE | 50 | 20 | 30 | 0 |
| ME | 50 | 20 | 30 | 0 |
| **合计** | **200** | **80** | **120** | **0** |

注：按学号每 50 个切块会得到 21/22/17/20，看着像「不均匀」，但那只是切块方式错了——学院是按 `sid mod 4` 交错分配的。按 DB 里的真实归属归组后，四院各 20、各拒 30，完全对称。

## 落库复核（drain 后）

```
/api/claim/dev/stat?resourceId=2
  totalQuota=200 dbRemain=120 redisRemain=120 drift=0 records=80 issued=80
  pending=0 invariantDelta=0 quotaInvariant=true balanced=true
  colleges: ART/CS/EE/ME 均 quota=20 dbRemain=0 redisRemain=0

quota_record JOIN quota_roster 按学院计数：ART 20 / CS 20 / EE 20 / ME 20
stock=120
```

## 结论（可引用）

在总名额 200、学院配额 4×20 的不对称配置下，200 并发一次性打满：
**80 成功、120 个 2002、0 个 2001，stock 剩 120。**
说明 stock 有余量时，超出的请求是被学院层精确拦下的——学院配额在并发下没有被穿透，也没有误伤（每院恰好 20 条落库、余量恰好 0）。

## 复位

`POST /api/claim/dev/reset?resourceId=2&quota=80` → 回到 `total=80 / σ=80` 基线。
