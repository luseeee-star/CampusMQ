# CampusMQ

单机消息 Broker，服务于校园限量资源发放场景（讲座名额 / 体育课等瞬时抢手资源）。
**教学验证项目，不自称生产级；投产应替换为 RocketMQ / Kafka。**

- 需求：《校园限量资源发放平台 · 需求规格说明书 v1.0》
- 排期：《CampusMQ 两周落地执行方案 v1.0》
- 排障记录：[LOGBOOK.md](LOGBOOK.md)

## 模块

| 模块 | 说明 |
|---|---|
| campusmq-common | 协议定义（16B 定长头）、Netty 编解码、CommitLog 消息二进制格式（62B 定长头） |
| campusmq-client | 生产者 / 拉模式消费者 SDK，opaque 异步转同步 |
| campusmq-broker | 服务端：CommitLog 顺序追加（mmap）、ConsumeQueue 20B 定长稀疏索引、Reput 异步建索引、崩溃恢复、消费进度持久化 |
| campusmq-demo | 校园名额发放业务 Demo（D10-D11） |
| benchmark | 压测与四组对照实验 E1-E4（D12-D13） |

## 构建与测试

```bash
mvn clean install          # JDK 21
```

测试覆盖：消息编解码 / CRC 坏数据识别、CommitLog 文件滚动与跨文件读取、
崩溃恢复（重启不丢、坏尾截断）、ConsumeQueue 索引、
端到端集成（收发、双消费组隔离、重启进度续费）。

## 命令行快速开始

```bash
# 1. 启动 Broker（-p 端口 -s 存储目录 -f 刷盘策略 SYNC|ASYNC_100MS|ASYNC_500MS）
java -cp <classpath> com.campusmq.broker.BrokerStartup -p 10911 -s ./store -f SYNC

# 2. 收发消息
java -cp <classpath> com.campusmq.client.example.QuickStart 127.0.0.1:10911
```

## 语义承诺（诚实边界）

- 投递语义：**至少一次**。ACK 确认 + 消费进度只进不退；幂等由业务侧唯一索引闭环
- 刷盘：SYNC 每条 force；ASYNC 定时 force——**异步策略下 kill -9 会丢尾部消息，丢失量将在实验 E2 量化**
- 顺序：同一 queueId 内分区有序，跨队列不保证
- 单机单副本，无 failover（明确非目标）
