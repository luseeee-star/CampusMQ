-- ============================================================
-- CampusSlot 运维报表（只读，D9 巡检用）
--
-- 用法：
--   docker exec -i cs-mysql mysql -uroot -pcampusslot -D campusslot < deploy/sql/report.sql
--
-- 本文件只允许 SELECT / SET，不得写任何数据。
-- 这里放的是「口径已经定死的查询」——口径写进 SQL 而不是只写在文档里，
-- 是因为文档会漂移，而报表每天跑的就是这份 SQL；两者不一致时，以 SQL 为准。
-- ============================================================

-- ------------------------------------------------------------
-- 常量：通知覆盖率的时间下界
--
-- 为什么分母必须带时间下界（不加就是假红灯）：
--   SYS-2 的通知写点是 2026-11-04 17:27:15 才接通全链路的
--   （`lock:3:S9Z2` 是第一条真实到达的通知）。此前的 quota_record
--   全部是 SYS-2 上线前产生的，它们**本来就不会有通知**。
--   若按"全量记录"当分母，r3 的 release 覆盖率是 3/16 = 19%，
--   一个纯粹由历史数据造成的假红灯，而且 13 条"缺失"永远补不回来。
--
-- 这个常量的含义：**从这一刻起，写点已经在运行，缺口才算缺陷。**
-- 换环境（重建库 / 换机器）时必须同步更新，否则分母会包含新一批历史数据。
--
-- 口径仍不充分的地方（必须同时写进报表注释，不能只靠人记）：
--   ① 锁定通知（lock）的分母只在「记录全部经由 /api/claim/mq 写入」时成立。
--      /api/claim 是同步写库路径（ClaimService.persist），压根不发 MQ 消息，
--      走它写入的记录永远不会有 lock 通知。当前 r3 里 S9Z1 就是这种样本，
--      它恰好落在下界之前才没有污染分母——这是巧合，不是保证。
--      → D10 之后若混着两个入口写同一个资源，lock 覆盖率必须按入口分样本，
--        不要在这个资源上算。
--   ② 分母用「事件发生时刻」（confirmed_at / released_at / claim_at）而不是
--      记录创建时刻：一条 17:00 锁定、17:40 确认的记录，它的 confirm 事件在下界之后，
--      应该进分母。
-- ------------------------------------------------------------
SET @t0 = '2026-11-04 17:27:00';

SELECT '=== 1. 通知覆盖率（分母带时间下界，见文件头常量说明） ===' AS section;

SELECT t.event,
       t.denominator,
       t.numerator,
       ROUND(t.numerator / t.denominator * 100, 1) AS pct
  FROM (
        SELECT 'lock' AS event,
               COUNT(*) AS denominator,
               SUM(n.id IS NOT NULL) AS numerator
          FROM quota_record r
          LEFT JOIN notification n
                 ON n.biz_id = CONCAT('lock:', r.resource_id, ':', r.student_id)
         WHERE r.resource_id = 3 AND r.claim_at >= @t0
        UNION ALL
        SELECT 'confirm',
               COUNT(*),
               SUM(n.id IS NOT NULL)
          FROM quota_record r
          LEFT JOIN notification n
                 ON n.biz_id = CONCAT('confirm:', r.resource_id, ':', r.student_id)
         WHERE r.resource_id = 3 AND r.confirmed_at >= @t0
        UNION ALL
        SELECT 'cancel',
               COUNT(*),
               SUM(n.id IS NOT NULL)
          FROM quota_record r
          LEFT JOIN notification n
                 ON n.biz_id = CONCAT('cancel:', r.resource_id, ':', r.student_id)
         WHERE r.resource_id = 3 AND r.released_at >= @t0 AND r.release_reason = 'CANCEL'
        UNION ALL
        SELECT 'timeout',
               COUNT(*),
               SUM(n.id IS NOT NULL)
          FROM quota_record r
          LEFT JOIN notification n
                 ON n.biz_id = CONCAT('release:', r.resource_id, ':', r.student_id)
         WHERE r.resource_id = 3 AND r.released_at >= @t0 AND r.release_reason = 'TIMEOUT'
       ) t;

-- 注：biz_id 的格式约定是 '事件:资源:学号'（NotificationMapper 类注释）。
-- 用等值而不是 LIKE 'confirm:%'，是为了让唯一键 uk_notify_biz_channel 的前缀生效，
-- 同时也顺带验证了"通知的 biz_id 确实按约定拼出来"这件事——
-- 若某天写点改了拼接方式，这里会直接掉成 0%，比 LIKE 前缀匹配更早暴露。

SELECT '=== 2. 通知状态分布 ===' AS section;

SELECT resource_id, status, COUNT(*) AS c,
       MIN(created_at) AS first_at, MAX(created_at) AS last_at
  FROM notification
 GROUP BY resource_id, status
 ORDER BY resource_id, status;
-- retry_count 的语义是「已尝试次数（含首次）」，retry_count=1 表示首次即成功。
-- 报表若要算「平均重试次数」，必须先减 1，否则全体虚高 1。

SELECT '=== 3. 库存平衡（DB 侧） ===' AS section;

SELECT r.id AS resource_id,
       r.total_quota,
       r.remain,
       SUM(CASE WHEN q.status = 'LOCKED'    THEN 1 ELSE 0 END) AS locked,
       SUM(CASE WHEN q.status = 'CONFIRMED' THEN 1 ELSE 0 END) AS confirmed,
       SUM(CASE WHEN q.status = 'RELEASED'  THEN 1 ELSE 0 END) AS released,
       r.total_quota - (SUM(CASE WHEN q.status IN ('LOCKED','CONFIRMED') THEN 1 ELSE 0 END)) AS expect_remain,
       (r.remain = r.total_quota - SUM(CASE WHEN q.status IN ('LOCKED','CONFIRMED') THEN 1 ELSE 0 END)) AS balanced
  FROM quota_resource r
  LEFT JOIN quota_record q ON q.resource_id = r.id
 WHERE r.id = 3
 GROUP BY r.id, r.total_quota, r.remain;
-- 更完整的平衡还要比 Redis 的 quota:stock:{id}，SQL 里读不到，D9 的 /progress 接口补。

SELECT '=== 4. 未修复的对账差异 ===' AS section;

SELECT id, resource_id, student_id, source, drift, detected_at, fixed_at
  FROM reconcile_diff
 WHERE fixed_at IS NULL
 ORDER BY detected_at DESC
 LIMIT 50;

-- ------------------------------------------------------------
-- 4.5 幂等键数 vs 记录数：**不相等是正常的**
--
-- 2026-11-04 18:11 实测：r3 有 21 条记录、20 个 `quota:idem:3:*` 键。差的 `S9Z1`
-- 不是丢键，是它的 TTL 到期了 —— 而且**两条链路的 TTL 口径本来就不同**：
--
--   同步入口 `/api/claim`（ClaimService:117）
--       TTL = confirm_timeout_sec + idem-ttl-extra-sec = 1800 + 600 = 2400s
--   异步入口 `/api/claim/mq`（AsyncClaimService.idemTtlMs）
--       TTL = max(confirmTimeout + extra, close_at − now + 1 天)  → r3 实测 680,909s ≈ 7.9 天
--
-- 异步链路必须长，理由写在 AsyncClaimService 的注释里：活动持续 7 天，
-- 学生在第 2 天重复点击时若键已过期，Lua 会当成全新申领再扣一次余量，
-- 直到 INSERT IGNORE 返回 0 才发现重复，中间还得回补。
-- 同步链路是 D2 的设计，那个时代还没有活动跨天的问题。
--
-- 后果：**同一个资源混用两个入口时，同步入口的记录在 40 分钟后就没有幂等保护**，
-- 学生能再抢一次（被 uk_record_resource_student 挡住 + 补偿回补，不会超发，
-- 但会浪费一次 Redis + DB 操作）。D9.5 若把两条链路混在同一资源上，
-- 这个差集会被误读成"丢键"。
--
-- 判定 idem 键状态只能去 Redis 读，**不要用 SQL 反推**：
--   redis-cli TTL quota:idem:{rid}:{sid}
-- 拿 expire_at + extra 去算会错，因为 expire_at 可以被外部 UPDATE 改过
-- （17:58 的边界实验就把 S9M3 / S9M4 的 expire_at 改成了 +1.5s / −100ms）。
-- 试过一次，算出"18 条应该过期、实际只缺 1 条"这种自相矛盾的结果，已删掉那个查询。
--
-- 结论：**账本在 stock，不在幂等键。** 平衡判据只有一条：
--   total_quota − (LOCKED + CONFIRMED) == remain == quota:stock:{id}
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- 5. D13 算时延时的两个坑（写成注释，因为写不成 SQL 判据）
--
-- ① 秒精度样本：2026-11-04 17:58:43 之前写入的 confirmed_at / released_at
--    仍是 DATETIME(0) 截断后的值（.000）。这些行的
--    "confirmed_at − claim_at" 含有最多 1 秒的假偏移，不能混进平均值。
--    识别方式：WHERE confirmed_at < '2026-11-04 17:58:43'  → 标注为秒精度样本。
-- ② updated_at 现在也是 (3)，但它与 confirmed_at 在同一批 UPDATE 里赋值，
--    两者差值应恒为 0；若看到非 0，说明有人改了外层的 SET 或加了触发器。
--    时延一律用 confirmed_at / released_at，不要用 updated_at——
--    它会被任何一次无关的 UPDATE 刷新（ON UPDATE CURRENT_TIMESTAMP(3)）。
-- ------------------------------------------------------------
