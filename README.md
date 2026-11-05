# CampusSlot · 校园限量资源发放中台

> 面向校内各部门的限量名额发放：**名额少、开放时刻固定、瞬时流量极高**，
> 一次发放必须做到不超发、不少发、可对账。
>
> 技术栈 **Java 21 / Spring Boot 3.3 / MySQL 8 / Redis 7 / RocketMQ 5.3**。
>
> 配套文档：《校园限量资源发放平台 · 需求规格说明书 v2.0》
> 开发排期见 **`校园限量资源发放平台_单人开发计划_14天.md`**（v1.2，2026-11-04；
> 仓库外单独维护一份，不在 `docs/` 下留副本，避免两份互相打架）
>
> v1.0「自研 CampusMQ Broker」方案已整体废弃，代码保留在 git 历史 `f4d107d`，
> 文档归档于 `docs/archive/`。

---

## 1. 当前进度（D4 完成 + 三项判据加固，D5 前置已就绪）

| 项 | 状态 |
|---|---|
| 三模块 Maven 骨架 | ✅ 可编译（`mvn clean package` 全绿，20 秒） |
| 依赖基线锁定 | ✅ Spring Boot 3.3.5 / MyBatis-Plus 3.5.9 / rocketmq-client-java 5.2.2 |
| 两个可启动进程 | ✅ `slot-app`(8080) / `slot-consumer`(8082) 均能独立启动 |
| 中间件编排 | ✅ 9 个容器实测全 Up，MySQL 12 张表、时区 +08:00、`max_connections=2000` |
| 数据模型 | ✅ `schema.sql` 10 张表，含总名额与学院配额两层防超发约束 |
| MQ 拓扑 | ✅ 3 个 Topic + 5 个消费组（含 dev 冒烟组），`topicRoute` 实测 16/8/4 队列正确 |
| 环境自检页 | ✅ `http://127.0.0.1:8080/` 可视化三中间件连通性与关键配置 |
| **D1 对照组① 裸 DB** | ✅ 200 并发 600 请求 → 超卖 **65–86 件**（`bench` profile 下），`remain` 为负 |
| **D2 对照组② Redis Lua** | ✅ 超卖 0、`drift` 0、连跑 5 轮稳定；吞吐 **390 req/s** |
| **D3 对照组③ MQ 异步落库** | ✅ 吞吐 **364 → 490 req/s（+35%）**，p99 **1269 → 970 ms（−24%）**，`pending` 归 0 |
| **D3 判据阳性对照** | ✅ Redis 灌 250 / DB 200 压 250：`oversold=0`（不超发）、`idemKeys=250`（少发 50 被看见） |
| **D4 幂等与首次结果（R-3）** | ✅ 同一学号连提 3 次，响应体**逐字节一致**，只落 1 条记录 |
| **D4 学院配额（G-1）** | ✅ 4 学院 × 20 名额打满 80，第 21 人返回 **2002**（全场仍有剩余） |
| **D3 故障实测** | ✅ 停掉消费者压 200 请求：`pending=200 / records=0`，重启后追平归 0 |
| **判据加固（D8 前置）** | ✅ `oversold` 去 clamp 改带符号；新增 `quotaInvariant`（`remain+issued==totalQuota`），D8 引进取消后不用改 |
| **落库去重加固** | ✅ 去掉 `INSERT IGNORE`，改 `DuplicateKeyException` 分流；约束错误不再被 ACK 吞掉 |
| **压测器上限标定** | ✅ 空转端点实测 **4500 req/s**（保守值），D3 的 490 只占其 11%，D5 曲线可信 |
| **D11 A1 建活动 + 发布** | ✅ `POST /api/admin/resources` + `/publish`：名单与学院配额随建活动写入，发布灌三类 key，重入不覆盖（`stock 9→9`、删 key 后只补缺）；四条拒绝路径实测 1001/1003/2007 |
| **D11 A2 进度 + 对账导出** | ✅ `GET /{id}/progress` 三套账并列（DB 余量 / Redis 余量 / 由记录反推的应有余额）+ `GET /{id}/export.csv`（配置口径与实际口径并排，「未分学院」桶显式保留）；`deptId` 可选过滤，与活动不符直接 1001 |
| **A 块验收 smoke** | ✅ 五条判据全绿（2026-11-05 傍晚）：名单外 1005 走 mq 入口；删三类 key 后重发各补 1、再发写入 0；`/drain` 后三数相等 `drift 0`；2002「你所在学院的名额已满」与 2001「名额已满」文案可区分；CSV 的 BOM 用 Excel 真打开验证过中文 |
| D5 拐点压测 | ⬜ 下一步：单 key 拐点 QPS + P99 曲线（Gatling 从这里开始上） |

## 2. 目录结构

```text
campusslot/
├── slot-common/                共享内核：错误码 / 统一响应 / 常量 / 枚举 / 实体 / Mapper / Redis Lua / MQ 封装
├── slot-app/                   API 侧唯一进程（8080），功能在包内分层：
│   └── com.campusslot.app/
│       ├── gateway/            身份解析 + traceId 落地（Servlet Filter）
│       ├── resource/           G-1~G-4 管理端四个接口：建活动、发布、进度、对账导出（A1+A2，2026-11-05）
│       ├── claim/              S-1~S-5：抢名额、确认、取消、我的记录             ← D2–D8
│       │   ├── ClaimService            对照组②：Redis 判定 + 同步落库
│       │   ├── AsyncClaimService       对照组③：Redis 判定 + MQ 投递（D3 起主链路）
│       │   ├── ResourceMetaCache       主链路上最后一次 DB 往返靠它搬走
│       │   └── ClaimDevController      reset / stat / drain 三个实验工具
│       ├── reconcile/          SYS-3：每 5 分钟巡检 Redis 与 DB 差异 + 出报表   ← D9
│       └── controller/         环境自检与 dev 端点
├── slot-consumer/              消费者进程（8082），可单独 kill 做故障注入
│   └── com.campusslot.consumer/
│       ├── record/             cg_record：唯一写 quota_record 的地方（D3）
│       └── DevSmokeConsumer    cg_smoke：D1 定时消息精度验证，独立消费组
├── deploy/                     compose / broker.conf / prometheus.yml / sql / topic-init.sh / reset.sh
│                               dev-server.ps1（启停服务） / experiment.ps1（一次调用跑完一轮实验）
├── benchmark/                  压测脚本与数据生成器（D5/D10 起换 Gatling）
├── docs/                       设计文档（archive 为已废弃 v1.0）
└── LOGBOOK.md                  每天「没按预期发生的事」
```

**没有 lottery**：需求书 §9 明确「不做抽签 / 摇号」。加回来必须同时补齐报名池留痕、
开奖链路、种子公示，否则不构成一个可验收的功能，只是多了一套没测过的代码。

**为什么只有 3 个模块**：一个人做，模块边界的收益（编译器拦住跨域调用）
撑不起十几次跨模块跳转的成本，所以功能边界改由包体现。
但 `slot-consumer` 必须独立成模块 —— 它要单独打包成可 `kill` 的进程，
用于 D6「停掉通知消费者」与 D11 故障注入，这个能力不能丢。
`slot-common` 也必须独立 —— 两个进程共用同一套实体、Mapper 和 Lua 脚本。

**为什么 3 个模块只起 2 个进程**：业务服务必须无状态，状态全在 Redis / MySQL / MQ 里，
所以进程数和模块数无关。真要按包拆成独立服务时，把包整体挪进新模块即可，类不用改。

## 3. 起环境

### 3.1 前置

- JDK 21、Maven 3.6+（本机 3.6.1 已验证可用，编译器插件锁 3.11.0）
- **Docker Desktop 需要先启动**（已验证：9 个容器全部 Up）

### 3.2 一键起中间件

```bash
cd deploy
cp .env.example .env        # 可选，改密码/端口
bash reset.sh               # down -v → up -d → 等就绪 → 建 Topic → FLUSHALL
```

> ⚠️ 本轮 schema 有**删表**改动（移除抽签两表与学分流水表）。
> `CREATE TABLE IF NOT EXISTS` 不会删旧表，**必须走 `reset.sh` 重建数据卷**，
> 否则旧卷里还留着那几张表，对账脚本会算出「申请数比实际多」。
> 详见 LOGBOOK「v2.0 需求改版」第 1 条。

> ⚠️ **Broker 的 store 不要放在快满的盘上。** 它有 75 / 85 / 90 三道磁盘闸，
> 超过就把所有写入拒成 `response-code=50001 … the broker's disk is full`，
> 看着像"Broker 挂了"。本机 D 盘常驻 90%，所以 `deploy/.env` 里把
> `RMQ_STORE_DIR` 指到了 C 盘。**换目录后 store 是空的，要重跑 `topic-init.sh`。**
> 详见 LOGBOOK「D3 第 0 条」。

| 组件 | 地址 | 说明 |
|---|---|---|
| MySQL | `127.0.0.1:3307` | 刻意错开 3306，避免和本机已装 MySQL 冲突；库 `campusslot`，root/`campusslot` |
| Redis | `127.0.0.1:6379` | `maxmemory-policy=noeviction`（库存 key 绝不能被淘汰） |
| RocketMQ Proxy | `127.0.0.1:8081` | **客户端连这里**，不是 9876 |
| NameServer | `127.0.0.1:9876` | 双节点 |
| Dashboard | http://127.0.0.1:8180 | 看 Topic / 消费组 / 堆积 |
| Prometheus | http://127.0.0.1:9090 | |
| Grafana | http://127.0.0.1:3000 | admin/admin |

### 3.3 起应用

**推荐用脚本**（`deploy/dev-server.ps1`，在仓库根目录执行）：

```powershell
# 起两个服务并等到真正就绪（默认 bench profile + MQ 打开）
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up

# 看状态：进程活着没有、端口在听没有、pid 是多少
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 status

# 停：按 pid 精确杀，不连坐别的 java 进程
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 down

# 看两个服务的日志尾部
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 log
```

它比手敲 `java -jar` 多做四件事：

1. 输出重定向到 `logs/app.log` / `logs/consumer.log`，**不接管道**——接管道时 `$LASTEXITCODE`
   拿到的是管道末端程序（`tee`）的，不是 java 的；
2. pid 落到 `logs/*.pid`，停的时候按 pid 杀，不用 `taskkill /F /IM java.exe` 连坐
   （那个会顺手杀掉 IDEA 编译进程和别的 java）；
3. 就绪判据是**日志里的标记**（`Tomcat started on port`），不是端口通。
   Tomcat 先绑端口再初始化 Spring，中间那段窗口端口是通的但接口全 503；
4. 起之前会摘掉重复的代理环境变量（见下表第一条）。

原始命令仍然可用，等价于 `-Profile` 传空：

```bash
java -jar slot-app/target/slot-app.jar
java -jar slot-consumer/target/slot-consumer.jar   # 另开终端
```

或 IDEA 里直接跑 `SlotApplication` / `ConsumerApplication`。
打开 **http://127.0.0.1:8080/** 是环境自检页：三个中间件的连通性、MySQL 时区、
Redis 内存与淘汰策略、MQ 是否真正启用。中间件没起时页面明确报错，而不是白屏。

### 3.3.0 为什么服务不能"跨调用存活"（实测，2026-10-27）

先说结论：**一套"起服务 → 反复调用做实验"的流程在本机跑不通**，必须压进一次调用。
`deploy/experiment.ps1` 就是干这个的（起服务 → 执行任意代码 → 无论成败都停服务）。

实测过程与证据：

| 做法 | 结果 |
|---|---|
| 前台 `java -jar` | 撞工具超时，且进程随调用结束而消失 |
| 后台任务托管 | 能跨调用存活，但永不退出 → 任务永远 pending，唯一的通知是"进程死亡"，看起来像"跑很久然后失败" |
| `Start-Process` 脱离托管 | 调用返回后再查：端口没了、进程没了、日志停在 `Tomcat started on port 8080`、**没有 shutdown 记录也没有异常** |

第三种是最能说明问题的：日志断在一行 INFO 上、err 文件为空，属于 `TerminateProcess`
级别的硬杀。也就是说执行环境在**每次调用结束时回收整个进程树**，脱不脱离托管都一样。

所以只有一条路：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 -Work @'
  $B = "http://127.0.0.1:8080"
  curl.exe -s --noproxy "*" -X POST "$B/api/claim/dev/reset?resourceId=1&quota=200" | Out-Null
  java benchmark/hammer/BaselineHammer.java $B 200 1 1 /api/claim/mq resourceId
'@
```

`-Work` 里可以是任意 PowerShell 代码，`finally` 保证两个服务一定被停掉。
代价是每轮固定多花约 20 秒启动，收益是**没有残留进程、没有后台任务通知、
不依赖"上一轮起的服务现在还活着"这种脆弱假设**。

### 3.3.1 Windows PowerShell 下的六个坑（实测）

在 PowerShell 里照 Linux 教程敲命令，会连续撞六个不明显的错误：

| 现象 | 原因 | 正确写法 |
|---|---|---|
| `bash: WSL ... execvpe(/bin/bash) failed` | PowerShell 的 `bash` 指向 WSL 入口 `C:\windows\system32\bash.exe`，没装 WSL 发行版 | `& "C:\Program Files\Git\bin\bash.exe" reset.sh` |
| `curl : 无法将"curl"项识别为 cmdlet` 或参数报错 | `curl` 是 `Invoke-WebRequest` 的别名，不认 `-X POST` | 写全名 `curl.exe -X POST ...` |
| 应用启动端口变成 49889 之类 | 系统里有 `SERVER_PORT` 环境变量，优先级高于 `application.yml` | `Remove-Item Env:SERVER_PORT -ErrorAction SilentlyContinue` |
| `curl.exe` 访问 127.0.0.1 返回 **502** 而不是连接拒绝 | 环境里设了 `http_proxy`，本机请求被代理转发出去 | 加 `--noproxy "*"` |
| `Start-Process : 已添加项。字典中的关键字:"HTTPS_PROXY"所添加的关键字:"https_proxy"` | 环境里同时存在大小写两份代理变量，PS 5.1 灌进大小写不敏感的字典时冲突 | 拉进程前先 `Remove-Item Env:https_proxy`；本地 java 服务不需要代理 |
| `xxx.ps1:105 字符: 1 表达式或语句中包含意外的标记"}"`，但那一行明明是赋值 | 脚本存成 UTF-8 **无 BOM**，PS 5.1 按 GBK 解码，中文全角标点（`：`= `EF BC 9A`）的尾字节把行尾换行吃掉，注释行和下一行粘成一行 | 脚本一律带 BOM 保存：`[System.IO.File]::WriteAllText($p,$t,(New-Object System.Text.UTF8Encoding($true)))` |

502 这个最坑：它看着像"应用起了但报错"，实际是**应用根本没起**，请求被代理拦了。
判断应用是否真的在跑，用 `netstat -ano | findstr :8080` 看有没有 LISTENING。

### 3.4 打开 MQ（D3 起主链路必需）

```bash
# 终端 1：API
java -jar slot-app/target/slot-app.jar --campusslot.mq.enabled=true
# 终端 2：消费者（不启动它，消息只会堆在 Broker 里，一条记录也落不下来）
java -jar slot-consumer/target/slot-consumer.jar --campusslot.mq.enabled=true
```

**默认 `campusslot.mq.enabled=false`**，此时 `MqProducer` 是只打日志的占位实现，
目的是让「没有中间件也能把服务跑起来」。一旦进入主链路联调与压测必须打开：
false 状态下申领会「看起来成功」但消息从未进 Broker，**所有吞吐与延迟数据作废**。

这条规则在 D3 之后从"建议"变成了"硬约束"，所以 `AsyncClaimService` 加了两道显式保护：

1. 启动时打一条 `ERROR`：`!! campusslot.mq.enabled=false：/api/claim/mq 主链路不会真的落库`
2. 请求进来直接返回 **9003**，不再"假装成功"

理由：异步接口离开 MQ 就没有任何意义，静默降级只会让一次配置疏忽变成一整天的假数据。

**压测**（`bench` 已是脚本默认 profile，直接 `up` 即可）

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 -Work "<压测代码>"
```

之所以不用「先 up 再另起一次调用压测」，见 3.3.0：进程活不过调用边界。

**演示学院配额（G-1，默认关闭）**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up `
    -AppExtra '--campusslot.claim.college-quota-enabled=true'
```

再加一个条件：该活动必须在 `quota_college` 里真的有行（seed 里 id=2 那场有）。
两个条件同时成立才会拦「学院满」（2002），见 `ResourceMeta#needCollegeQuota`。

**「拦名单外的人」是另一条判据** `ResourceMeta#needRosterCheck`，只看活动要不要名单，
**不需要开这个开关**。但它靠「从 `quota:roster:{rid}:{sid}` 取到学院编码」来判成员，
所以名单每一行都必须带 `college_code`——只给学号的名单灌完一个 key 都没有，全场 1005，而且不报错。

### 3.5 管理端四个接口（D11 / A1 + A2，2026-11-05）

建活动、发布、进度、对账导出。**目前没有鉴权**（系统里还没有管理端身份体系，写"只能建本部门的活动"只是装饰，
伪造成本为零；上线前必须先补——眼下任何人都能把全校名单导走）。

```bash
# 1) 建草稿（G-1 命名/名额/窗口 + G-2 名单 + 学院配额一次带进来）
curl --noproxy '*' -X POST http://127.0.0.1:8080/api/admin/resources \
  -H 'Content-Type: application/json' -d '{
    "deptId": 1,
    "title": "A1 验收·纯名单活动",
    "totalQuota": 10,
    "openAt":  "2026-11-06T09:00:00",
    "closeAt": "2026-11-06T18:00:00",
    "rosterRequired": true,
    "roster": [
      {"studentId": "2024000001", "collegeCode": "CS"},
      {"studentId": "2024000002", "collegeCode": "EE"}
    ]
  }'

# 2) 发布：灌三类 key（余量 / 学院余量 / 名单）→ 迁移状态 → 清元信息缓存
curl --noproxy '*' -X POST http://127.0.0.1:8080/api/admin/resources/4/publish
```

**四条必须知道的规则**（都是实测踩出来的，不是风格问题）：

1. **`deptId` 必填且必须是启用部门**。`quota_resource.dept_id` 没有外键，传 99 也能插进去；
   接口会先 `selectById` 判存在且 `status=1`，不满足回 1001。
2. **`rosterRequired=true` 时，名单每一行都要带 `collegeCode`**，缺了直接 1001 并指出第几行。
   灌名单会跳过空编码的行，整份名单灌完 0 个 key、发布照样成功、抢的时候全场 1005。
   没有单独的"导入名单"接口，所以建的时候不写就没有第二次机会——`rosterRequired=true` 而名单为空同样 1001。
3. **发布按状态分流**：`DRAFT` 覆盖写（值取数据库的 `remain`，用来校正上一次残留）；
   `PUBLISHED` 只补缺（`SETNX`，有人在抢，绝不能覆盖）。顺序固定为**先灌 Redis、最后迁状态**，
   中途失败时活动还停在 `DRAFT`，重跑能走覆盖分支。返回体的 `onlyIfAbsent` 告诉你这次走了哪条。
4. **库存 key 丢失后补缺，值取 `remain`，可能有偏差**：如果此刻消费端还没追平，
   `remain` 比真实余量**大**，补进去等于把在途的又放一遍（实测 `pending=-2 / drift=+2`）。
   接口会打 P0 error 并在 `notes` 里提示：发布完先 `/api/claim/dev/drain`，再看 `stat.drift` 是否为 0。

发布完立刻抢一次不能出现「活动还没开放」——那说明 `ResourceMetaCache.evict` 没生效，
缓存要等 5 秒才自然过期，演示现场看起来像发布没成功。

#### 进度与对账导出（A2）

```bash
# 进度：一次看全三套账
curl --noproxy '*' http://127.0.0.1:8080/api/admin/resources/2/progress

# 对账表：默认按学院分行；groupBy=none 只出合计
curl --noproxy '*' -o reconcile.csv \
  "http://127.0.0.1:8080/api/admin/resources/2/export.csv?groupBy=college"

# 两个接口都收可选 deptId；传了就必须和活动所属部门一致，否则 1001（返回 0 会被误读成“没人领”）
curl --noproxy '*' "http://127.0.0.1:8080/api/admin/resources/2/progress?deptId=2"
```

**`/progress` 为什么不是一个余量数字**：这个系统里"还剩多少"有三个答案，不等就说明问题出在哪一层。

| 字段 | 含义 | 不等的含义 |
|---|---|---|
| `dbRemain` | 数据库看还剩多少 | 异步链路里它**滞后**于 Redis（消息还没落库） |
| `redisRemain` | 学生实际能抢到的 | 这才是抢名额那一刻的真相 |
| `expectRemain` | 总名额 − (已锁定 + 已确认) | 和 `dbRemain` 不等 → `remain` 这一列被绕过改过 |

`drift = redisRemain − dbRemain`：> 0 是在途消息（压测刚停时正常），< 0 是 DB 多扣了（回补撞车或手工改 Redis），必须进对账。
`stockKeyMissing` 单独给一个布尔位：库存 key 不存在时抢占会返回 `-3`「系统繁忙」，那是"活动挂了"而不是"发完了"。
`rosterRows` 与 `rosterKeys` 不等就是名单漏灌——漏灌是静默的（学生被判 1005），只能靠这两个数对出来。

**`dbBalanced` 这个名字里的 db 不能省**：它只说"数据库自己能不能对上"，不含 Redis 那一侧。
库存 key 被删时它是 `true`、`redisMatchesDb` 是 `false`——两个字段一起看才知道系统好不好，叫 `balanced` 会误读。

**导出的两套口径不要混成第三个数**：

- **配置口径**（`quota_college`）：这个学院最多给几个；
- **实际口径**（`quota_record LEFT JOIN quota_roster`）：这个学院实际领走几个。

两张表没有可靠的一一对应关系（配额可以不覆盖所有学院，名单也可能写了配额里没有的编码），硬 JOIN 会丢行。
`quota_record` **没有 `college_code` 列**，所以学院维度是"**名单上写的**"，不是"发那一刻扣的"——
报告里引用这列数必须先说明来源。

「未分学院」那一行（`college_code` 为空）**是真实存在的桶，不是空值噪音**：`roster_required=0` 的活动
（r1、r3）整份记录都 join 不上名单，手工插的记录也 join 不上。三份来源取**并集**、`null` 单独成一桶排在最后、
**参与合计**。让 `GROUP BY` 吃掉它，各学院加起来就会比总数少，看起来像丢了几条记录。

## 4. 环境变量与可调参数

| 变量 | 默认 | 说明 |
|---|---|---|
| `DB_HOST` / `DB_PORT` / `DB_NAME` | 127.0.0.1 / 3307 / campusslot | |
| `DB_USER` / `DB_PASS` | root / campusslot | |
| `REDIS_HOST` / `REDIS_PORT` | 127.0.0.1 / 6379 | |
| `MQ_ENABLED` | false | |
| `MQ_ENDPOINTS` | 127.0.0.1:8081 | Proxy 的 gRPC 端点 |
| `DEV_ENDPOINTS` | true | `/api/dev/mq/**` 验证端点，试点上线前必须关 |
| `CONFIRM_TIMEOUT_SEC` | 1800 | R-5 确认时限。**演示超时释放不要改代码**，启动时传 60 即可 |
| `IDEM_TTL_EXTRA_SEC` | 600 | 幂等键比确认时限多活的冗余，必须 > 0 |
| `RECONCILE_INTERVAL_MS` | 300000 | SYS-3 对账巡检间隔，5 分钟 |
| `NOTIFY_MAX_RETRY` | 3 | SYS-2 通知重试上限 |
| `META_CACHE_TTL_MS` | 5000 | D3 资源元信息缓存 TTL。**不要调到分钟级**：运营下架活动后学生最长还能抢这么久 |
| `COLLEGE_QUOTA_ENABLED` | false | G-1 学院配额总开关。关掉后 `quota_college` 只出报表不拦截（D12 决策点） |
| `CONSUME_THREADS` | 20 | cg_record 消费线程数。天花板是 Topic 队列数 16，D10 量拐点 |

## 5. 数据模型要点

10 张表，三处是「不加就一定会出事」的约束：

| 表 | 关键约束 | 拦住什么 |
|---|---|---|
| `quota_resource` | `remain >= 0`、`remain <= total_quota` | 超发（R-1）、回补把余量加超总量 |
| `quota_college` | `remain >= 0`、`remain <= quota` | 学院配额超发；不建行 = 该学院不限（不是「配额为 0」） |
| `quota_record` | `UNIQUE(resource_id, student_id)` | 一人多份（R-2）、重复投递、重试 |

其余：`dept`、`quota_roster`（G-2 名单）、`quota_backfill_queue`（R-6 补位用的表结构——
**只建了表，没有任何业务写入，当前 0 行**，见 §6 末尾的说明）、
`notification`（带 `next_retry_at`）、`idempotent_log`（带 `record_id`，支撑 R-3 复现首次结果）、
`reconcile_diff`、`quota_report`（R-7 每次发放的可归档报表）。

## 6. MQ 拓扑

| Topic | 队列数 | 消费组 | 用途 |
|---|---|---|---|
| `quota_claimed` | 16 | `cg_record`（tag=CLAIM） | **异步落库，唯一写 `quota_record` 的地方**（D3） |
| `quota_claimed` | 16 | `cg_smoke`（tag=smoke\|delay） | dev 调试消息，D1 定时精度验证，不碰数据库 |
| `quota_claimed` | 16 | `cg_notify` | 锁定通知（D8） |
| `quota_release_delay` | 8 | `cg_release` | SYS-1 **计划中**的定时消息；当前超时释放只由扫表兜底（`fixedDelay=500ms`），本 Topic 尚无发送点 |
| `quota_backfill` | 4 | `cg_backfill`（无实现） | R-6 补位定向通知 —— **未实现**：本 Topic 只登记在 producer 的 `setTopics` 里（为满足"发之前必须声明"的约束），没有任何业务代码调用发送；`slot-consumer` 里也没有对应消费者 |

**同一条消息被三个消费组各收一份**，这是 RocketMQ 的消费组语义（组内分摊、组间广播）。
D3 之后这三个组的分工变得很具体：`cg_record` 写入、`cg_smoke` 只在 dev 用、`cg_notify` D8 才接。
**停掉其中任何一个不影响其他两个**——D10「停通知消费者不影响主链路」就是靠这个结构成立的。

对账**不走 MQ**：§5.3 的 SYS-3 是每 5 分钟的定时任务。做成事件流会得到一条
「对账消息本身也要被对账」的链路，纯属自找麻烦。

**当前唯一一条不满足的需求是 R-6（释放的名额进补位队列、定向通知）。** 上面两张表里
`quota_backfill_queue` 和 `quota_backfill` 都只是占位：表是空的，Topic 没有任何发送点和消费者。
释放掉的名额会回到余量池，下一个来抢的人正常拿到，但**不会主动通知排队的人**。
这条是明确未做，不是"待测"或"部分完成"。

## 7. MQ 三个验证点怎么验（D1 遗产，别删）

`POST /api/dev/mq/send-delay?delaySeconds=37` 发一条 **37 秒**后投递的消息：

1. **任意时间定时消息是否真的生效** —— 37 秒不在 4.x 的 18 个固定档位里。
   如果 30 秒就到了，说明走的是老客户端路径，D5 才发现就是灾难。真实到达时刻看
   `slot-consumer` 日志里的 `bornLatency` 与 `lateBy`。
2. **Topic 队列数是否生效** —— `bash deploy/topic-init.sh` 会回显每个 Topic 的读写队列数，
   不是 4 才算对。
3. **链路是否通** —— 能发出去、消费者能收到。

这些手工消息**只被 `cg_smoke` 收**（tag 过滤 `smoke||delay`），不会落到 `quota_record`。
D3 之前两者共用一个消费组，一旦消费者开始写库，每调一次调试接口就往库里塞一条垃圾消息。

## 8. 已踩的坑（详见 LOGBOOK）

- MyBatis-Plus **3.5.9 起 jsqlparser 被拆包**，`PaginationInnerInterceptor` 直接编译失败。
- `rocketmq-client-java` 5.x 四处签名与直觉不符：builder 是顶层类、`setDeliveryTimestamp` 只收 long 毫秒、
  `setTopics` 是可变参数、`getMessageId()` 返回对象。
- **删表类 schema 变更不会自动生效**，`CREATE TABLE IF NOT EXISTS` 不是「对齐」。
- Lua 返回值改成数组后，`StringRedisTemplate` 拿回的是 `List<String>`，不是 `List<Long>`。
- **SQL 日志会同时掩盖性能和竞态**：`StdOutImpl` 开着时裸版超卖 54–67 件，关掉后 65–86 件。
  打印发生在持有连接期间，连接周转变慢、有效并发被压低。对照实验必须固定日志配置，
  压测一律加 `--spring.profiles.active=bench`。
- **Git Bash 会把 `/api/...` 参数改写成 Windows 路径**，传给压测器的 path 会变成
  `D:/.../PortableGit/api/claim`。要么 `export MSYS_NO_PATHCONV=1`，要么直接用 PowerShell。
- `HttpRequest.newBuilder()` 若写在 try 外，URL 拼错时线程直接死、`err` 不计数，
  统计结果反而显示 `error=0`。
- **Broker 磁盘使用率超阈值时拒绝所有写入**，客户端报 `InternalErrorException … 50001
  service not available now … the broker's disk is full [CL: 0.91 …]`。
  本机 D 盘本身已用 90%，而 `diskMaxUsedSpaceRatio` 原设 88% —— **MQ 从来没写成功过一条消息**。
  这个错误看起来像"Broker 挂了"，实际是"活着但拒写"，处置方式完全不同。
- **`kill -9` 消费者后重启，队列要等旧连接心跳超时才重新分配**：192 条消息秒级追平，
  最后 8 条卡了 46 秒。短超时的 drain 会把这种情况误报成"丢消息"。
- **第一轮压测永远是预热轮**。D3 首轮 336 req/s，同参数后三轮 505–515 req/s。
  差异来自 JIT、连接池和元信息缓存，不是代码。**首轮数据不进报告。**
- **`INSERT IGNORE` 会把 CHECK 失败 / 外键失败 / 数据超长一起降级成「受影响行数 0」**，
  于是被当成「消息重投」ACK 掉 —— 消息永久消失，日志里只有一条 warn。
  `NOT NULL` 缺字段更阴：它插入隐式默认值并返回 **1**，脏数据落库还回报成功。
  改用普通 INSERT + `DuplicateKeyException` 分流，代价见 §9。
- **`ON DUPLICATE KEY UPDATE` 不能替代它**：Connector/J 默认 `useAffectedRows=false`，
  冲突时返回的是 found rows（1 而不是 0），会把重复判成插入成功。
- **压测器本身会先饱和，而且长得和业务拐点一模一样**（都是吞吐不再上升）。
  不先标定就只能靠猜。空转端点 `/api/dev/ping` + 扫并发曲线，成本十分钟。
- **同一档并发的两轮数据可以差 40%**（c=100 实测 4279 vs 3131，c=800 实测 7198 vs 5873）。
  Windows 桌面环境本身不是一个稳定的测试台。每档至少 2 轮，报中位数或最低值，
  **不要拿单轮的最高值当结论**。

## 9. 三组对照数据

### 组⓪ 压测器自身上限（先测尺子，否则后面全白测）

D5 要找「单 key 的 Redis 拐点」，这条曲线是报告的核心数字之一。
但如果 hammer 自己先饱和了，测到的「拐点」是**压测器的**，不是 Redis 的——
**而且两者长得一模一样**（吞吐不再上升），光看曲线分不出来。

所以先用同一个 hammer 打一个空转端点 `/api/dev/ping`，链路上只有
Tomcat + Spring MVC + 消息转换，不碰 DB / Redis / MQ：

```bash
curl "http://127.0.0.1:8080/api/dev/ping/reset"
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 30 0 /api/dev/ping none
curl "http://127.0.0.1:8080/api/dev/ping/count"   # 应 == 200×30 = 6000
```

**这条线要扫，不能打一炮看峰值**——hammer 用平台线程，并发越高上下文切换越贵，
吞吐可能在某一点反着往下走。每档跑 2 轮，取第 2 轮（第 1 轮在建连和 JIT）：

| 并发 | 吞吐 req/s | p50 | p99 | 服务端命中 |
|---|---|---|---|---|
| 50 | 2918 | 3 ms | 359 ms | 1500 ✓ |
| 100 | 3131 | 6 ms | 677 ms | 3000 ✓ |
| 200 | **4545** | 9 ms | 735 ms | 6000 ✓ |
| 400 | 4352 | 20 ms | 1897 ms | 12000 ✓ |
| 800 | 5873 | 34 ms | 2093 ms | 24000 ✓ |

**上限保守取 4500 req/s**（200 档两次都在 4500 附近，最稳；800 档更高但噪音极大）。

三个结论：

1. **D3 的 490 req/s 只有这个上限的 11%。** 371 那个数字从来不是天花板，
   业务链路的瓶颈完全在别处（Redis Lua + 同步发 MQ）。
2. **D5 的 Redis 拐点只要低于 4000，就远在压测器天花板之下，曲线可信。**
   这一条是 D5 开工的前置条件，不是可选项。
3. **p99 的真相在这里。** 空转端点在 400 并发就堆到 1.9 s，而 D2/D3 的 p99 是 1.3 s。
   说明业务链路那 1.3 s 里**有一大块是排队等待，不是 Redis 慢**。
   下次有人问「为什么 P99 这么高」，先看这条基准线再回答。

> 服务端命中数必须等于 `并发 × 每线程次数`。对不上说明客户端侧有请求根本没发出去，
> 那一轮的吞吐是被**低估**的，拿它当上限会算低。

### D1 裸 DB（对照组①）

```bash
curl -X POST "http://127.0.0.1:8080/api/baseline/reset?activityId=1&quota=200"
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3
curl "http://127.0.0.1:8080/api/baseline/stat?activityId=1"
```

判据只有一条：**`remain < 0`**，超卖件数 = `-remain`。
（`records` 与 `consumed` 恒等，不要把它们当成两个独立校验。）

| 配置 | 并发 | 请求 | 成功 | remain | 超卖 |
|---|---|---|---|---|---|
| 默认（SQL 日志开） | 50 | 150 | 150 | +50 | 0 ← 请求数不够，没压到边界 |
| 默认（SQL 日志开） | 200 | 600 | 255 / 255 / 267 | −55 / −55 / −67 | 55 / 55 / 67 |
| **`bench`（日志关）** | 200 | 600 | 286 / 271 / 265 | −86 / −71 / −65 | **86 / 71 / 65** |
| 默认（SQL 日志开） | 400 | 800 | 275 | −75 | 75 |

日志一关超卖反而变多，原因见 LOGBOOK「日志会掩盖竞态」。

**为什么单独建 `bare_*` 两张表**：`quota_resource` 上有 `CHECK (remain >= 0)`，
裸版在并发下会把 remain 改成负数，MySQL 直接抛异常 —— 你拿到的是报错，
不是「超卖 86 件」这个数字。详见 `deploy/sql/baseline.sql` 文件头。

### D2 Redis Lua 同步落库（对照组②）

```bash
curl -X POST "http://127.0.0.1:8080/api/claim/dev/reset?resourceId=1&quota=200"
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3 1 /api/claim resourceId
curl "http://127.0.0.1:8080/api/claim/dev/stat?resourceId=1"
```

判据：**`quotaInvariant == true` 且 `drift == 0`**。

`quotaInvariant` 是 `dbRemain + issued == totalQuota`。**不要用 `records > totalQuota`**——
D8 引入取消之后，取消会返还名额，「发了取消、取消再发」会让累计 `records`
合法地远超名额，那个判据会天天误报；而 cancel 让 remain 加 1、issued 减 1，
两边反向变化，不变式不动，**D8 那天这条不用改**。

注意主表有 `CHECK (remain >= 0)`，超卖不会表现为负数，套用 D1 的判据会读错。

**D2 买到的是正确性，不是性能。** 判定搬进了 Redis，落库仍然是两次同步写，
DB 往返一次没少，吞吐与 D1 持平（370 req/s 上下）。

### D3 Redis + MQ 异步落库（对照组③）

```bash
curl -X POST "http://127.0.0.1:8080/api/claim/dev/reset?resourceId=1&quota=200"
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 3 1 /api/claim/mq resourceId
curl "http://127.0.0.1:8080/api/claim/dev/drain?resourceId=1&timeoutMs=60000"   # 必须先等 pending 归 0
curl "http://127.0.0.1:8080/api/claim/dev/stat?resourceId=1"
```

判据：**`pending = 0` 且 `oversold = 0` 且 `balanced = true`**。

`pending = (total − redisRemain) − (total − dbRemain)`，是 D3 才有的量：
正数=消息还在队列里，0=账平，负数=DB 比 Redis 多扣（见下面"模糊态"）。

**同机、同 profile、同参数（200 并发 × 3 = 600 请求，容量 200），交替各跑 5 轮：**

| 组 | 轮次 | 成功/已满 | 吞吐 req/s | p50 | p99 | pending | 超卖 |
|---|---|---|---|---|---|---|---|
| D2 同步 | 1–5 | 200 / 400 ×5 | 427 / 350 / 364 / 451 / 351 | 251 / 212 / 134 / 145 / 118 | 1194 / 1517 / 1345 / 1071 / 1269 | 0 | **0** |
| D3 异步 | 1–5 | 200 / 400 ×5 | 413 / 499 / 479 / 490 / 498 | 78 / 57 / 57 / 54 / 53 | 1151 / 972 / 993 / 970 / 827 | 0 | **0** |

**取中位数**（每组各有一个明显的预热轮，均值会被它带偏）：
吞吐 **364 → 490 req/s（+35%）**，p50 **145 → 57 ms（−61%）**，p99 **1269 → 970 ms（−24%）**。

> 这组是在 store 迁到 C 盘之后重测的。迁盘之前（store 在 D 盘）测过一组 390 → 511，
> 结论方向一致、幅度略有差异。**换环境必须重测，不能拿旧数字配新结论。**

三条结论：

1. **吞吐涨了 35%，不是数量级。** 因为 D3 没有让"往返"消失，只是换了一个：
   原来的 `selectById + INSERT + UPDATE` 三次 DB 往返，换成一次同步 MQ 发送。
   只要发送是同步的，600 个请求就还是要排队 600 次网络往返。
   **真正的数量级提升要靠减少往返次数（D7 分片、批量落库），不是靠换个中间件。**
2. **p50 降得最多（−61%），p99 降得少（−24%）。** 中位数受益于"请求线程不再等 DB 锁"，
   尾部仍然被 Hikari 的 64 连接和 Broker 的发送队列拖着。这说明**尾部延迟的瓶颈已经换人了**：
   D2 卡在数据库，D3 卡在 MQ 发送 + 消费侧连接池。
3. **D3 的收益有一半不在压测数字里。** 真正的变化是"消费侧可以独立扩容"：
   `slot-app` 扩到 8 台时，落库能力不用跟着扩，把 `cg_record` 的消费线程和副本加上去就行。

### 判据本身的阳性对照（证明它不是瞎的）

只看 `oversold = 0` 是**不可信**的：`records` 因为任何原因偏小（没打满、口径错、消息全丢了）
都会得出 0 或负数。所以在信任何一组数据之前，先做两次阳性对照：

**对照①：直接改 Redis，看 stat 能不能发现**

```bash
docker exec cs-redis redis-cli SET quota:stock:1 250     # DB 仍是 200
curl "http://127.0.0.1:8080/api/claim/dev/stat?resourceId=1"
# → drift: 50, pending: -50, balanced: false    ✅ 判据是活的
```

**对照②：拆掉第一层防线，看 DB 兜不兜得住**

Redis 灌成 250、DB 仍是 200，然后压 250 个请求（每个新学号）：

| 观察项 | 值 | 说明 |
|---|---|---|
| 接口返回"抢到" | **250** | Redis 被灌了，全部放行 |
| `records` / `issued` | **200** | DB 的 `UPDATE … WHERE remain > 0` 挡住了后 50 个 |
| `oversold` | **0** | **没有超发** —— 第二层防线成立 |
| 消费端 `P0 库存漂移` | **50** 条 | 每一条被挡下的消息都留了痕 |
| `idemKeys` | **250** | 250 个学生被告知"抢到了" |
| `idemMatch` | **false** | ← **这才是"少发 50"的信号** |
| `balanced` | **false** | 三维判据，不是只看 pending |

**只看 `records` 和 `oversold` 时这一轮"一片太平"（200 / 0）**，
只有幂等键数（250 vs 200）能暴露"50 个人被告知成功、实际没有记录"。
这就是 `balanced` 必须先看 `idemKeys == records` 的原因。

### D3 的难点：投递成功但落库失败，账还对得上吗

**实测（把消费者进程真杀掉，不是改配置）：**

```bash
# 1. 复位后杀掉消费者进程（只杀 8082 那个）
taskkill //F //PID <slot-consumer 的 PID>
# 2. 压 200 请求，每个学号一次
java benchmark/hammer/BaselineHammer.java http://127.0.0.1:8080 200 1 1 /api/claim/mq resourceId
```

那一刻的账：

```json
{"ok":200,"records":0,"dbRemain":200,"redisRemain":0,"drift":-200,"pending":200,"balanced":false}
```

- **接口 200 个请求全部返回"抢到"，但一条记录都没落库。** 这就是异步化的真实代价，
  必须在报告里说清楚：`LOCKED` 这个状态在接口返回的瞬间是"承诺"，不是"事实"。
- `pending = 200` 精确地量化了这个缺口，`drift = -200` 是它的镜像。
- 重启消费者后：`drain` 返回 `pending=0`，`records=200`、`drift=0`、`balanced=true`。
  **账自己对上了，没有人工干预。**

**为什么会自己平**：消费端只有两种动作——落库成功就 ACK，DB 暂时不可用就返回 `FAILURE` 让
Broker 重投。重投时 INSERT 撞上唯一索引抛 `DuplicateKeyException`，说明这条已经处理过，
直接 ACK、不再扣余量。所以"至少一次投递"被收敛成"恰好一次落库"。

**那个消不掉的模糊态**：同步发送超时时，客户端知道"没收到确认"，但不知道 Broker 收没收。
此时回补 Redis 会让 `pending` 变成**负数**（Redis 认为没扣、DB 认为扣了）。
这不是 bug，是分布式系统的固有窗口——D3 的处理方式是让它可被观测，
**由 D9 对账把它查出来并修回去，而不是假装它不存在**。

### 落库去重：`INSERT IGNORE` 会把不该吞的错误一起吞掉

D3 的第一版用 `INSERT IGNORE` + 「受影响行数为 0 就是重复」。这句话在**只有唯一键冲突**时成立，
但 MySQL 的 `IGNORE` 是一个**五合一开关**：

| 被吞的错误 | 实际行为 | 后果 |
|---|---|---|
| 唯一键冲突 | 跳过该行，affected = 0 | 期望的「重复」✓ |
| `CHECK` 约束失败 | 跳过该行，affected = 0 | 被误判成重复，ACK 掉，**消息永久消失** |
| 外键失败 | 跳过该行，affected = 0 | 同上 |
| `NOT NULL` 缺字段 | 插入隐式默认值，affected = **1** | 更糟：脏数据落库，还回报成功 |
| 数据超长 | **截断**后插入，affected = **1** | 学号被截成 32 字符，静悄悄存进去 |

2026-10-27 实测（60 字符学号打 `VARCHAR(32)`）：

```
WARN ClaimPersistService - 落库失败，交给 MQ 重试 resourceId=1
  studentId=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
  reason=Data truncation: Data too long for column 'student_id' at row 1
```

改法：去掉 `IGNORE`，用普通 INSERT，靠**异常类型**分流：

```java
try {
    tx.execute(...);                    // 冲突就抛，不再返回 0
} catch (DuplicateKeyException e) {
    return DUPLICATE;                   // 唯一索引拦下 → 真正的重复，ACK 且不回补
} catch (DataAccessException e) {
    return TRANSIENT;                   // CHECK / 外键 / 超长 / 连接问题 → MQ 重试
}
```

**为什么不用 `ON DUPLICATE KEY UPDATE`**：Connector/J 默认 `useAffectedRows=false`（返回 found rows），
冲突时它返回 **1 而不是 0**，会把重复直接判成插入成功 —— 同一个坑换了个位置，而且更难看出来。

两个必须接受的副作用：

1. 约束类错误现在会重试到 Broker 的 16 次上限后**进死信**。这比静默丢失好（可检测、可修复），
   但**死信队列得有人看**，否则只是把「静默丢失」换成了「静默堆积」。D9 对账要覆盖这个来源。
2. 同步链路 `/api/claim` 也同步改了：约束错误现在返回**系统错误**并回补名额 + 删幂等键，
   而不是 2006「你已申领过」。告诉学生「重复申领」在那是错的，他重试多少次都还是 2006。

> **顺带暴露的入口缺陷**：`/api/claim/mq` 不做学号长度校验，60 字符能扣掉一个 Redis 名额、
> 发出一条注定失败的消息。正确的做法是在入口拒绝（`studentId` 长度/字符集校验），
> 而不是让非法输入走到数据库层才被拦。**D5 之前补。**

### D4 幂等与首次结果（R-3）

```bash
curl -X POST "http://127.0.0.1:8080/api/claim/dev/reset?resourceId=1&quota=200"
# 同一个学号连提 3 次，比对响应体
for i in 1 2 3; do
  curl -i -X POST "http://127.0.0.1:8080/api/claim/mq?resourceId=1&studentId=2023D4TEST"
done
```

实测（2026-10-27）：

| 次 | 响应体 | `X-Claim-Replay` |
|---|---|---|
| 1 | `{"code":0,"message":"ok","data":{"resourceId":1,"status":"LOCKED","remain":199}}` | （无） |
| 2 | `{"code":0,"message":"ok","data":{"resourceId":1,"status":"LOCKED","remain":199}}` | `1` |
| 3 | 同第 2 次 | `1` |

- **响应体逐字节一致**，`remain` 都是 199（首次扣减后的余量），不是"现在的余量"。
- 落库结果：`records=1 / issued=1 / pending=0 / balanced=true` —— 三次请求只产生一条记录。
- 幂等键 TTL 实测 **691196 秒（≈8 天）** = 活动结束时刻 + 1 天。

**两个必须做对的地方：**

1. **幂等键的 value 不能是 `"1"`。** 存 `"1"` 只能回「你已申领过」，而需求书 §4.2 明确反对这句话。
   存「首次扣减后的余量」，重复请求才能原样复现首次结果。
2. **重放分支绝对不能发消息。** 重放意味着这次没有真的扣减；若照样投一条，
   消费端 `INSERT IGNORE` 返回 0 → 被判定为重复 → 回补 Redis 余量 →
   **明明什么都没发生，余量却多了一个**。这就是"重放导致超卖"，而且只在真人重复点击时触发，
   压测（每次一个新学号）永远测不到。

**幂等键 TTL 为什么从 40 分钟改成 8 天**：D2 用 `确认时限 + 10min`。异步链路下这个值不够——
活动持续 7 天，学生第 2 天重复点击时幂等键已过期，Lua 会当成全新申领再扣一次，
直到消费端撞唯一索引才发现是重复，中间多扣的那次还要回补。**与其在错的时候补救，不如让它别错。**

### D4 学院配额（G-1）

```bash
# 启动时必须打开总开关（默认关）
java -jar slot-app/target/slot-app.jar --campusslot.mq.enabled=true \
     --campusslot.claim.college-quota-enabled=true
# 资源 2：80 名额，4 个学院各 20，200 人资格名单（每院 50 人）
curl -X POST "http://127.0.0.1:8080/api/claim/dev/reset?resourceId=2"
```

实测（CS 学院连抢 22 次，全场还有 80 个名额）：

| 次 | 返回 | 说明 |
|---|---|---|
| 1–20 | `code=0`，`remain` 79→60 | 学院配额 20 个，正常扣减 |
| 21–22 | **`code=2002`** | 「你所在学院的名额已满」——而**全场还剩 60 个** |
| 名单外学号 | **`code=1005`** | 资格名单前置（P4） |

再把 EE / ME / ART 各 20 人打满，最终：

```json
{"totalQuota":80,"dbRemain":0,"redisRemain":0,"records":80,"pending":0,"oversold":0,"balanced":true,
 "colleges":[{"college":"CS","quota":20,"dbRemain":0,"redisRemain":0}, "…EE/ME/ART 同"]}
```

**五条必须讲清楚的设计：**

1. **`-4`（学院满）与 `-1`（全场满）必须是两个码。** 混用会让学生看到"名额已满"
   而进度条显示还有剩余，然后反复刷新投诉（R-8 明确要求「不得静默失败/提示含糊」）。
2. **学院配额必须和总余量在同一个 Lua 里判。** 先扣总余量、再在 Java 里判学院，
   学院满时总余量已经被扣掉了，拒绝之后必须补回去——只要有一条失败路径忘了补，
   名额就永久少一个，而且不报错，最后表现为「明明还剩 5 个名额，全校都提示系统繁忙」。
3. **学院配额默认关闭。** D12 才决策它值不值得做（16 分片 + 换片重试 + 学院配额，
   复杂度会相乘）。现在留的是"实现了、可开启、默认不动"。
4. **学院编码来自 `quota:roster:{rid}:{sid}` 单 key**，不是在 Lua 里动态拼 key。
   学院开启时才多一次 GET，默认路径零额外开销。
5. **名单判定只对 `/api/claim/mq` 生效，而且当前还挂在学院配额开关上。**
   同步入口 `/api/claim` 用的 `claim_basic.lua` 里**一次名单判定都没有**，不管开关怎么配，谁都能抢；
   异步入口的 `useCollegeQuota()` 要求 `roster_required` 与 `college-quota-enabled` **同时**成立
   （缓存层再与一次「有没有学院行」），而开关默认 `false` —— 所以只配名单、不配学院配额的活动，
   名单等于不存在，且不会报错。验证 P4 必须用异步入口 + 显式打开开关。
   > **已改（A1，`f966b86`）**：判定已经拆成 `needRosterCheck()`（查不查名单）和 `needCollegeQuota()`（扣不扣学院配额），
   > `roster_required=1` 不再要求配学院配额。上面这段描述的是 D7 当时的状况，留作历史——
   > 它解释了后来为什么"只配名单的活动"这个形态以前根本走不通。
   > 同步入口不补名单判定——它是 D3 的对照组，多一次 GET 会污染对照组数字。

### 顺带验证的兜底

| 场景 | 操作 | 期望 |
|---|---|---|
| P0 库存缺失 | `redis-cli DEL quota:stock:1` 后申领 | **`9005`**，不能是 `2001` |
| 消息格式坏掉 | 手工投一条非 JSON 到 tag=CLAIM | 消费端 **丢弃 + ERROR**，不能返回 FAILURE（会堵队列） |
| MQ 未开 | 不带 `--campusslot.mq.enabled=true` 打 `/api/claim/mq` | **`9003`**，不能"假装成功" |

第一条最容易写错：把「key 不存在」和「余量为 0」都当 `nil/0` 处理，
学生会被通知「名额已满」然后放弃，而名额其实还在。
**错的方向是「误报已满」，代价是白白损失名额。**

## 10. 下一步（D5 · 压测基线 + 找拐点）

D3/D4 把三个对照组和两条语义都做完了，但**所有数据都是"固定 200 并发"这一个点**。
一个点画不出曲线，也答不了面试官最常问的那句「这本机压测的极限在哪」。

**开工前置条件已满足**（三件事在 D5 之前做完，都在 §9）：

| 前置 | 状态 | 为什么必须在 D5 之前 |
|---|---|---|
| 判据改为 `quotaInvariant` | ✅ | 否则 D8 一引取消，超卖会在数据里消失，而你会以为是优化生效了 |
| 落库去重改异常分流 | ✅ | 否则约束错误被 ACK 吞掉，D5 的曲线里混着一批"丢了但没人知道"的消息 |
| 压测器上限标定 | ✅ 4500 req/s | 否则测到的拐点可能是压测器的，而且和业务拐点长得一模一样 |

D5 要做的：

1. 换成 Gatling（或 JMeter 兜底），阶跃加压：50 → 100 → 200 → 400 → 800 → 1600 并发；
2. 每个并发点记录 **QPS / p50 / p99 / 错误率**，画出拐点；
   **每档先减掉压测器基线**（组⓪ 那条线），否则你测的是两段加起来的成本；
3. 拐点出现后逐个嫌疑排查：Tomcat 400 线程、Hikari 64 连接、Lettuce 64 连接、
   Broker 发送队列、消费侧 16 队列。**要指认出是哪一个先到顶，而不是笼统说"到瓶颈了"**。

现有工具够用：`/api/claim/dev/stat` 已经能读出 `pending`，
`/drain` 的 `costMs` 就是"堆积追平耗时"，D10 的消费能力曲线直接拿它当纵轴。

**还有一件小事要在 D5 之前补**：`/api/claim/mq` 目前不校验 `studentId` 长度，
60 字符能扣掉一个 Redis 名额并发出一条注定失败的消息（§9 实测）。
非法输入该在入口被拒，不该走到数据库层才拦。

现有工具够用：`/api/claim/dev/stat` 已经能读出 `pending`，
`/drain` 的 `costMs` 就是"堆积追平耗时"，D10 的消费能力曲线直接拿它当纵轴。

本机压测的局限必须写进报告：**压测端与被测端同机，数据只用于相对对照，不代表绝对容量。**
