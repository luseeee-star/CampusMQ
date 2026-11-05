/**
 * 对账巡检与报表（需求书 §5.3 SYS-3 / §5.2 G-4）。
 *
 * <p>包级分层，不是独立 Maven 模块。它跑在 {@code slot-app} 进程内，
 * 靠 {@code @EnableScheduling} 驱动，因此多实例部署时必须靠条件更新幂等。
 *
 * <p>落地日：D5 / D8。Redis 与 MySQL 之间没有连接，主链路又是「Redis 判定、MQ 异步落库」，
 * 所以两者必然存在瞬时不一致。目标不是「永远一致」，而是
 * <b>不一致时 5 分钟内被发现，并且能被解释或自动修复</b>（NFR-4）。
 *
 * <p>对账口径（每 5 分钟，{@code campusslot.reconcile.interval-ms}）：
 * <pre>
 *   Redis 余量  vs  (total_quota - DB 已发放数)      已发放 = LOCKED + CONFIRMED
 *   差异 != 0 → 写 reconcile_diff + 告警 + 给出修复建议
 *   每次巡检结束 → 追加一行 quota_report（R-7：每次发放都要有可归档报表）
 * </pre>
 *
 * <p>两类差异必须分开看，否则修错方向：
 * <ul>
 *   <li>Redis 比 DB 少：消息还没落库，属正常延迟，下一次巡检通常自己就平了</li>
 *   <li>Redis 比 DB 多：可能有人工改过 Redis、或释放回补重复执行了，这类才是真问题</li>
 * </ul>
 *
 * <p>注意：这是定时任务，2→8 台时会重复执行。修数据必须靠条件更新，不能靠「只有一个实例在跑」。
 *
 * <p>同时承担：DLQ 巡检、消费堆积水位巡检、通知送达率统计。
 */
package com.campusslot.app.reconcile;
