# B7 补测 · 磁盘满之后重启能不能起来（2026-11-09 17:50–17:52）

## 待验证的原句

原来 B7 写的是：

> AOF incr 文件被扩到全长、数据没落盘。这正是崩溃后要靠 `aof-load-truncated yes`
> 才能起来的形态。

这句话当时**没有做过重启**，是推测。而且它的前半句（"比设备还大"）已经证伪：
`67,086,187 B = 66.99 MiB`，上限 64 MiB = `67,108,864 B`，文件比上限**小** 22,677 字节。

## 第一次尝试失败：`--tmpfs` 不跨容器重启

```
docker restart b7-redis  →  dbsize 0
```

看起来像"AOF 恢复失败"，实际不是：`--tmpfs` 是内存挂载，**容器停止时被销毁**。
重启后 `/data/appendonlydir` 里只剩三个刚创建的空文件
（`base.rdb` 89 B、`incr.aof` 0 B、`manifest` 88 B），67MB 的 AOF 根本不存在了。

**这条方法学错误要记下来**：用 `--tmpfs` 做"重启后能否恢复"的实验，测的一直是"文件还在不在"，
而不是 AOF 的完整性。

## 第二次：容器不停止，只杀 Redis 进程

`--tmpfs` 在容器存活期间是持久的，所以把容器拆成"`sleep infinity` 保活 + redis-server 子进程"，
用 `pkill -9` 模拟崩溃、再原地拉起，`/data` 就保住了。

```bash
docker run -d --name b7b --tmpfs /data:size=64m redis:7-alpine \
  sh -c 'redis-server --port 6379 --dir /data --appendonly yes --appendfsync everysec > /tmp/r1.log 2>&1 & exec sleep infinity'
docker exec b7b redis-benchmark -n 200000 -d 1024 -r 1000000 -t set -q     # 灌满
docker exec b7b sh -c 'pkill -9 redis-server; sleep 1; redis-server … > /tmp/r2.log 2>&1 &'
```

### 结果 A（第一次杀进程）

| | 值 |
|---|---|
| 崩溃前 `dbsize` | 60,902 |
| `r1.log` | `# Short write while writing to the AOF file: (nwritten=588, expected=1069)` |
| 重启后 `dbsize` | **60,901**（少 1 条） |
| `r2.log` | `DB loaded from incr file appendonly.aof.1.incr.aof: 0.155 seconds`，**无任何 warning/error** |

### 结果 B（复跑，带大小测量）

| | 崩溃前 | 重启后 |
|---|---|---|
| `dbsize` | 60,843 | **60,834**（少 9 条） |
| incr AOF 字节 | 67,092,601 | **67,092,601（一字未变）** |
| `/data` 可用 | 4 KB | 4 KB |
| 日志 | — | `DB loaded from incr file … 0.159 seconds`，无 truncate 警告 |

## 结论（替换掉原来的推测）

1. **能起来。** 两次都正常加载、`Ready to accept connections`，没有 `Bad file format`、
   没有 `short read` 警告，**不需要人工干预**。所以"要靠 `aof-load-truncated yes` 才起得来"
   这个说法**不成立**——`aof-load-truncated` 默认就是 yes，而且加载器在这一档数据上压根没报错。
2. **丢的是 AOF 缓冲区里没落盘的那几条：1 条 / 9 条，不是整段。** 两次都表现为
   `dbsize` 略小于崩溃前，而不是归零。`everysec` 的窗口里最多丢约 1 秒的写入，
   与 B3 的结论（SIGKILL 下已 fsync 的扣减零丢失）方向一致。
3. **AOF 文件长度不变，没有 ftruncate。** 结果 B 的大小前后完全相同，说明这次崩溃时
   最后一条命令是**整条没写进去**（缓冲区里），而不是写了一半——所以文件尾恰好落在
   命令边界上，加载器自然读得下去。结果 A 出现了真正的"短写"（588/1069 字节），
   加载器也照样起来了、只是丢了那 1 条。**两种情况都不需要人工修 AOF。**
4. **重启不算"数据回来了"。** 设备仍然是 100% 满（可用 4KB），`SET probe v` 能成功
   只是因为这条刚好塞进剩下 4KB；再写就该重新 `MISCONF`。

## 附带发现：磁盘满时 `docker stop` 关不掉，会拖满 10 秒宽限期

第一次用 `docker restart` 时，日志里有：

```
Received SIGTERM scheduling shutdown...
* Saving the final RDB snapshot before exiting.
# Write error while saving DB to the disk(rdbSaveRio): No space left on device
# Error trying to save the DB, can't exit.
# Errors trying to shut down the server. Check the logs for more information.
```

Redis 收到 SIGTERM 后要存一份最终 RDB，写失败就**拒绝退出**；Docker 只能等满 10 秒宽限期再
SIGKILL（`docker restart` 实测 10.7 秒）。也就是说：**磁盘满时"优雅停机"会退化成硬杀**，
而且那 10 秒里进程还在，端口还在，但已经不能写了。

对 `cs-redis` 的意义：`docker stop` 在磁盘满时不能当成"安全退出"用，
停机预算要按"10 秒 + SIGKILL"算。

## 边界

- 旁路容器的 `base.rdb` 是 89 字节的空 RDB，真实环境里 base 是几十 MB 的 RDB
  （`BGSAVE` 产物），加载路径会多一段；本测试只覆盖 incr AOF 的尾部。
- 单机上仍然无法验证"宿主机磁盘满 → MySQL 一起不可用"（vhdx 共用一块盘），
  这条继续按"未验证"写。
