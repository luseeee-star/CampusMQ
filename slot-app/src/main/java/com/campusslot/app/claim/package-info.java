/**
 * 申领与状态流转链路（需求书 §4.2 / §5.1）。
 *
 * <p>包级分层，不是独立 Maven 模块。字段、Mapper、Lua 脚本在 {@code slot-common}，
 * 因为消费者进程写库时也要用同一套。
 *
 * <p><b>两个申领入口，别混</b>（2026-11-04 踩坑后补，那天用错了入口，把"没有生产者"读成了"消费者不工作"）：
 * <ul>
 *   <li>{@code POST /api/claim} → {@code ClaimService}：Redis 扣减 → <b>同步 INSERT 记录 + UPDATE 余量</b>
 *       → 返回。{@code persist} 在请求线程里跑，<b>完全不发 MQ 消息</b>。它是 D2/D3 的对照组
 *       （用来量"不用 MQ 的同步版"的 RT 与吞吐），也是 dev 里种数据的便捷路径。</li>
 *   <li>{@code POST /api/claim/mq} → {@code AsyncClaimService}：Redis 扣减 → 发 {@code quota_claimed}
 *       → 立即返回；落库由 {@code cg_record} 异步完成。通知（{@code cg_notify}）、对账、
 *       D10 故障矩阵全部依赖这条链路。</li>
 * </ul>
 * 结论：<b>要验证任何消息驱动的消费者，只能打 {@code /api/claim/mq}</b>。
 * 用 {@code /api/claim} 去等消费者会得到一个"链路不通"的错误结论——那条链路上没有生产者。
 * 排查时先确认一件事：这段被测代码，到底有没有写点？
 *
 * <p><b>由此推出的一条统计口径</b>：锁定通知（{@code lock:*}）只可能由 {@code /api/claim/mq} 产生，
 * 同步入口 {@code /api/claim} 永远不会有对应通知。所以 D9 算"通知覆盖率"时，分母必须限定为
 * MQ 入口产生的记录，否则会得到 2/3 这种假红灯。两种入口的样本在 {@code quota_record}
 * 里目前无法区分（没有 source 列可依赖），最省事的做法是选一个只走 MQ 入口的资源做样本，
 * 而不是事后从混在一起的记录里剔除。
 *
 * <p><b>同一条资源上，两个入口的幂等键 TTL 也不同</b>（2026-11-04 实测，r3）：
 * 同步入口 {@code TTL = confirm_timeout_sec + 600 = 2400s}；
 * 异步入口 {@code TTL = max(confirmTimeout + 600, close_at − now + 1 天)} ≈ <b>680,909s（7.9 天）</b>
 * ——活动跨天时键不能在第 40 分钟就失效，否则第 2 天重复点击会被 Lua 当成全新申领再扣一次余量。
 * 后果：混用两个入口时，<b>同步入口写下的记录在 40 分钟后就没有幂等保护</b>，
 * 学生重复点击会被 {@code uk_record_resource_student} 挡住并走补偿回补（不会超发，
 * 但白花一次 Redis + DB 操作，补偿失败还会漏一个名额）。
 * 这同时是「D9 不要拿幂等键数当账本」的原因：键数比记录数少，可能不是丢键，是 TTL 到了。
 *
 * <p>落地日：D3–D6。MQ 版抢票主链路顺序：
 * <ol>
 *   <li>网关限流（超量 → 3001，明确提示，不静默失败）</li>
 *   <li>校验活动状态与资格名单（未开放 / 不在名单 → 1004 / 1005）</li>
 *   <li>Redis EVALSHA {@code claim.lua}：幂等判定 + 总名额扣减 + 学院配额扣减，一次原子操作</li>
 *   <li>同步发送 {@code quota_claimed}（不用 sendOneway）。
 *       计划中的 {@code quota_release_delay} <b>还没有发送点</b>：超时释放目前只由扫表兜底</li>
 *   <li>立即返回「已锁定，请在 30 分钟内确认」</li>
 * </ol>
 *
 * <p>返回值映射（对应 {@code claim.lua} 的 {code, remain}）：
 * <pre>
 *   { 0, N}  → 申领成功，剩余 N
 *   {-1, 0}  → 名额已满
 *   {-2, N}  → 重复请求，用 N 复现首次结果（R-3：不能回「你已参与过」）
 *   {-3,-1}  → 库存或学院配额 key 缺失，P0 告警，按系统繁忙处理，绝不能当作「已满」放过去
 *   {-4, 0}  → 本学院配额已满（但全场还有余量），文案必须与「名额已满」区分
 * </pre>
 *
 * <p>确认（S-3）与取消（S-4）都走「条件更新 + 受影响行数」：
 * 取消必须验证持有关系（status = LOCKED 才能释放），否则重复取消 10 次会让余量虚增（R-4 / §8.1）。
 *
 * <p>注意：<b>MQ 版</b>不在同步链路里写 MySQL——落库由 {@code slot-consumer} 的 cg_record 完成，
 * 因为主链路只负责「名额归属落定」这一件事。但 {@code /api/claim} 那个对照组是同步写库的
 * （见上面的两个入口），不要把这条约束套到它身上。
 */
package com.campusslot.app.claim;
