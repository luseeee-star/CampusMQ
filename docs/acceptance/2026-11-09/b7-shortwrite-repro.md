# B7 补测三 · 短写会不会留下残缺 AOF，以及"重启后能写"到底是什么（2026-11-09 18:26–18:31）

## 要判的两个问题

`b7-aof-restart.md` 里有两条解释是**推断**，不是观测，必须用实验替换：

1. **短写是否让文件尾部残缺？** 原文写「588 落在两条命令之间」「写失败的 481 字节从未进入文件」。
   但 `expected=1069` 恰好是一条 1KB `SET` 的长度（`-d 1024`），
   `*3\r\n`(4) + `$3\r\n`(4) + `SET\r\n`(5) + `$16\r\n`(6) + key(16) + `\r\n`(2) +
   `$1024\r\n`(6) + value(1024) + `\r\n`(2) = **1069**。
   `expected` 是 `sdslen(aof_buf)`，1069 = 一条命令，说明当时 buffer 里只有这一条，
   588 落在**命令中间**——按原解释，加载器必然报 `short read` 并 truncate。这与「无警告、长度不变」互斥。
2. **重启后 `SET probe v` 成功，是因为"刚好塞进剩下 4KB"吗？**

## 方法

同前：一次性容器 `--tmpfs /data:size=64m` + `appendonly yes` + `appendfsync everysec`，
`redis-benchmark -n 200000 -d 1024 -r 1000000 -t set` 灌满，容器保活 + `pkill -9 redis-server` 重启。
这次把**崩溃前后文件长度、`r1.log` 短写行数、`r2.log` 全文、文件尾部字节、`size mod 1069`** 全部记下来。

## 结果一：短写（部分写）**不会**留下残缺文件

崩溃前：

```
incr_size=67,099,015      tmpfs 64.0M 已用 100%，可用 0
dbsize=60,789
short_write_lines=1
# Short write while writing to the AOF file: (nwritten=1657, expected=7483)
misconf_lines=0
```

注意 `expected=7483 = 7 × 1069`（buffer 里 7 条），`nwritten=1657 = 1069 + 588`——
**落点在第 2 条命令的中间**，和上一轮那个 588 是同一个位置（上一轮 1657−588 也是 1069 的整数倍）。

杀进程重启后：

| 观测 | 值 |
|---|---|
| incr 文件长度 | 67,099,015 → **67,099,015**（一字未变，`ftruncate` 没发生） |
| `dbsize` | 60,789 → **60,783**（少 **6** 条） |
| `r2.log` 的 warning / error / truncat / short | **零匹配** |
| 加载日志 | `DB loaded from incr file appendonly.aof.1.incr.aof: 0.156 seconds` → `Ready to accept connections` |
| `size mod 1069` | **23** |
| 尾部字节 | 以 `\r\n` 结尾（`… L 7 0 U l 5 g f \r \n`） |

### 自洽的算术

```
67,099,015 − 23 = 67,098,992 = 62,768 × 1069
```

即文件 = **23 字节头 + 62,768 条完整 `SET`**，尾部停在一条命令的结尾。
**文件是完整的**，加载器当然不会报 `short read`。

### 机制

`flushAppendOnlyFile` 在部分写之后并不会丢掉剩下的数据：它把已写字节数从缓冲区前部切掉
（`sdsrange(aof_buf, nwritten, -1)`），剩下的留在 buffer 里，下一次 flush 接着写。
所以**短写只是把一次 write 拆成两次，不产生残缺命令**；`Short write` 是一条错误日志，
不是"文件坏了"的宣告。

真正丢掉的那 **6 条**，是 `pkill -9` 瞬间还在 buffer 里、尚未 flush 的**新**命令——
和短写无关，和 `everysec` 的窗口有关。

## 结果二：重启后能写，不是"刚好塞进 4KB"，是状态位被复位

重启后（磁盘仍然 100%，可用 0）连续写 8 条 1KB：

```
初始：aof_last_write_status=ok     df /data 可用 0     dbsize=60,783
#1  set=OK                                                    aof_write=ok    availKB=0
#2  set=OK                                                    aof_write=err   availKB=0
#3  set=MISCONF Errors writing to the AOF file: No space left on device   aof_write=err
#4..#8  同上，全部 MISCONF
最终 dbsize=60,785（+2，对应 #1 #2 被接受）
```

**`availKB` 全程是 0**，所以"刚好塞进剩下 4KB"不成立。真实机制：

1. 重启把 `aof_last_write_status` 复位成 `ok`；
2. 命令执行完、追加到 `aof_buf` 就返回 `OK`，**flush 发生在返回之后**；
3. `#1` 的 flush 成功（tmpfs 还有零头页），`#2` 的 flush 失败 → 状态置 `err`，但 `#2` 的 `OK` 已经发出；
4. 从 `#3` 开始，`writeCommandsDeniedByDiskError` 生效，直接 `MISCONF` 拒写。

所以**重启后存在 1–2 条写入的「假成功」窗口**：返回 `OK` 但 AOF 没落盘，再重启就丢。
这条对运维有实际意义——磁盘满之后重启，`SET` 返回成功不能当作"空间已恢复"的证据。

## 结论（替换原推断）

1. **短写不留残缺文件**：部分写后 Redis 保留缓冲区剩余、下一次 flush 继续，文件始终是连续完整命令序列。
   证据：`23 + 62,768 × 1069` 精确等于文件长度、尾部 `\r\n`、加载零告警、长度一字未变。
2. **丢的 6 条来自 kill 瞬间未 flush 的新命令**，不是短写丢的。`everysec` 窗口内最多丢约 1 秒写入。
3. **`aof-load-truncated` 与本次无关**：既然尾部完整，加载器没进截断分支（见 `b7-aof-truncation.md`
   对"真残缺"的构造实验）。
4. **重启后第一次写成功不能作为磁盘恢复的判据**：状态位复位 + 返回早于 flush，构成 1–2 条的假成功窗口。
