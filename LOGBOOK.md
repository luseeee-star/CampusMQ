# LOGBOOK · 每天没有按预期发生的事

> 这个文件记录排障过程本身——它证明代码是被调试过的。

## D0-D5（2026-10-20，存储引擎阶段一次跑完）

### 1. 原骨架是 war 包，不符合多模块设计
初始仓库是 IDEA 生成的 Maven webapp 骨架（packaging=war + junit 3.8.1），
与执行方案 §2.1 的五模块结构完全不匹配。推倒重建为聚合父工程。
教训：环境准备阶段就该先对齐包结构，而不是沿用 IDE 默认骨架。

### 2. Maven 3.6.1 在 JDK 21 上能跑，但插件版本有坑
maven-compiler-plugin 3.13.0+ 要求 Maven >= 3.6.3，本机 3.6.1 装不上；
退回 3.11.0 + source/target 21 组合编译通过。surefire 3.2.5 运行正常，
但 dumpstream 会提示 boot jar 含绝对路径（无害警告）。
待办：正式压测前把 Maven 升到 3.9.x，避免实验数据被工具链问题污染。

### 3. ServerHandler 忘标 @Sharable，第二条连接直接拉取超时
集成测试里生产者（第一条连接）收发全部正常，消费者（第二条连接）pull 永远超时。
日志翻到 `ChannelPipelineException: ServerHandler is not a @Sharable handler` 才定位到。
教训：Netty 单例 handler 共享给多个 channel 必须标 @Sharable；
"第一条连接正常"是极具迷惑性的假信号，排障要先看服务端日志而不是客户端。

### 4. 文件滚动死循环：文件没满但放不下这条消息
getLastMappedFile 只在 wrotePosition >= fileSize 时才滚动。
4KB 文件写到 4050 字节、下一条 90B 放不下时：isFull()=false → 返回同一文件 →
append 失败 → 重试三次 → 抛异常。改成"append 失败即强制 rollNewFile()"。
教训：`isFull` 和 `能放下这条消息` 是两个不同的谓词，滚动条件应该由后者驱动。

### 5. physicalOffset 在编码之后才设置，消息头里永远是 0
putMessage 先 encode 再进循环里改 physicalOffset——但偏移量写进了消息头（§3.2 格式），
编码后修改字段不影响已序列化的字节。所有消息的 physicalOffset 头字段恒为 0，
messageId（brokerHost:port:offset）也全部相同，幂等去重键直接作废。
修法：先定位目标文件算出物理偏移、再编码；滚动后偏移变了要重新编码。
教训：二进制格式里任何字段都可能在编码后变化，序列化必须是最后一步。

### 6. 崩溃恢复把"正常滚动空洞"误判成坏尾
恢复算法最初写的终止条件是 `lastValid < fileSize` 就截断后续文件。
但文件滚动天然产生"写 4050/4096 字节后换新文件"的空洞——第二个文件是完好数据，
却被当坏尾整文件丢弃（恢复测试只剩 43/100 条）。
修法：decode 读不下去时检查剩余区域是否全零——全零=从未写过=正常滚动，
含非零字节=写了半条=真坏尾。只有后者才截断。
教训："丢弃尾部"的边界必须精确到字节语义，不能靠"没写满"这种粗粒度判断。

### 7. 手工 classpath 漏了 netty-resolver，connect 无限挂起
命令行跑 QuickStart 时连接挂死。Maven 测试里 netty-resolver 是传递依赖自动带上，
手工 -cp 漏了它 → Bootstrap.connect 需要的 AddressResolverGroup 找不到 →
异常被吞进 Netty 内部，表现为"连不上但也不报错"。
教训：脱离构建工具手拼 classpath 时，先 `dependency:build-classpath` 生成再改。

### 8. 集成测试目录跨运行残留
测试存储目录写死在 target/test-store 下，mvn 不 clean 时上次的 offset/消息还在，
第二次跑必然失败。加了测试前置 TestStoreCleaner。
