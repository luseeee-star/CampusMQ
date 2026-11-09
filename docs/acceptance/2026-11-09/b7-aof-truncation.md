# B7 补测二 · `aof-load-truncated` 到底管不管这次的事（2026-11-09 18:10–18:15）

## 要判的问题

`b7-aof-restart.md` 那两次进程级重启「能起来、无告警、AOF 长度一字未变」。
由此有一句判断：原稿「要靠 `aof-load-truncated yes` 才起得来」**不成立**。

但这句话有两种可能的错法，必须分开：

- **错法一**：机制讲反了 —— 其实 `yes` 就是「截断尾命令然后继续」，`no` 是「直接拒绝启动」；
- **错法二**：归因错了 —— 机制没错，但那两次重启压根没走到截断这一步。

`b7-aof-restart.md` 的观测（长度不变、无 `short read` 警告）本身指向「错法二」，
但那是**排除法**，不是正面证据。这次直接构造残缺 AOF 来正面测。

## 方法

一次性容器 `b7v`（`--tmpfs /data:size=64m`、`appendonly yes`、`appendfsync everysec`，
容器保活 + `pkill -9 redis-server` 只杀进程），先写 3 个 key 让 AOF 有内容，然后在
`appendonlydir/appendonly.aof.1.incr.aof` **尾部追加半条命令**：

```
printf "*3\r\n\$3\r\nSET\r\n\$7\r\nhalfke" >> appendonly.aof.1.incr.aof
```

即 `SET halfke <value>` 缺了 value 和结尾 CRLF，文件从 110 字节变成 133 字节。
然后分别用默认配置和 `--aof-load-truncated no` 启动。

## 结果 A：默认（`aof-load-truncated yes`）

```
加载后文件长度=110        ← 追加的 23 字节被 ftruncate 回去
dbsize=3                  ← 原有 3 个 key 全在，半条命令被丢弃
PONG
```

日志（`/tmp/r_yes.log` 原文）：

```
* DB loaded from base file appendonly.aof.1.base.rdb: 0.000 seconds
# !!! Warning: short read while loading the AOF file appendonly.aof.1.incr.aof!!!
# !!! Truncating the AOF appendonly.aof.1.incr.aof at offset 110 !!!
# AOF appendonly.aof.1.incr.aof loaded anyway because aof-load-truncated is enabled
* DB loaded from incr file appendonly.aof.1.incr.aof: 0.000 seconds
* Ready to accept connections tcp
```

## 结果 B：`--aof-load-truncated no`

```
exit code = 1
ping → Could not connect to Redis at 127.0.0.1:6379: Connection refused
文件长度=133              ← 没被截断，原样留着
```

日志尾部：

```
# Unexpected end of file reading the append only file appendonly.aof.1.incr.aof. You can:
  1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>.
  2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.
```

## 结论

1. **机制成立，而且是可逆的一对**：`yes`（默认）→ 打三行 warning、`ftruncate` 回上一条完整命令的
   边界、继续启动；`no` → 拒绝启动、文件原样保留、并明确提示「要么 `redis-check-aof --fix`，
   要么把它设回 `yes`」。所以原稿「要靠 `aof-load-truncated yes` 才起得来」在**尾部真的残缺**时是
   **对的**，不是错的。
2. **但 `b7-aof-restart.md` 那两次并没有走到这一步。** 如果是走截断路径，必然同时满足三件事：
   长度变短、有 `!!! Truncating ... at offset N !!!`、有 `short read` 警告。实测三件都没有
   （67,092,601 → 67,092,601，日志只有 `DB loaded from incr file ...`）。所以那两次是
   **尾部完整**，加载器根本没进截断分支。
3. **两句话要拆开说**：
   - 「重启后能起来、不需要人工修 AOF」——**成立**，当日两次实测；
   - 「本次是被 `aof-load-truncated yes` 救回来的」——**不成立**，当日没触发；
   - 「尾部残缺时必须靠 `aof-load-truncated yes`（否则 `redis-check-aof --fix`）」——**成立**，本次构造实验实测。
4. **为什么短写没留下残命令**：`Short write (588/1069)` 里 `expected=1069` 恰好就是一条 1KB `SET`
   的长度，说明当时 buffer 里只有这一条，**588 落在命令中间而不是命令边界**。
   部分写之后 Redis 保留 buffer 剩余、下一次 flush 接着写，所以短写只是把一次 write 拆成两次，
   文件最终仍是连续完整的命令序列——不是"481 字节从未进入文件"。
   这一点已复现并定量确认（`b7-shortwrite-repro.md`：文件 `23 + 62,768 × 1069` 精确、
   尾部以 `\r\n` 结尾、加载零 warning）。

结论：**磁盘满导致的崩溃恢复，风险落在「最多丢 1 秒内未 fsync 的写入」和「若恰好截断到命令中间，
则必须由默认的 `aof-load-truncated yes` 静默截断（会丢那半条命令、日志留三行 warning）」，
不需要人工 `redis-check-aof`**——除非有人把该选项改成 `no`。
