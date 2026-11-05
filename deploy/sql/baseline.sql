-- ============================================================
-- D1 对照组：故意会超卖的裸数据库版本
-- ============================================================
--
-- 【这两张表是实验材料，不是业务表。不要修，不要加约束。】
--
-- 为什么不能直接用 quota_resource / quota_record 做这个实验：
--   quota_resource 有 CONSTRAINT ck_resource_remain CHECK (remain >= 0)，
--   裸版「先读后改」在并发下会把 remain 改成 -1，MySQL 直接抛 CHECK violation。
--   你拿到的是一个异常，而不是「超卖 37 件」这个数字 —— 而那个数字才是 D1 的全部产出。
--
--   quota_record 有 UNIQUE KEY (resource_id, student_id)，它保护的是 R-2「一人一条」，
--   与「不同人抢到超过总名额」是两回事，拦不住超卖，反而会让人误以为已经有保护。
--
-- 因此这里的四张约束全部故意缺失：
--   无 CHECK(remain >= 0)           → 允许 remain 变成负数，超卖可见
--   无 CHECK(remain <= total_quota) → 同上
--   无 UNIQUE(resource_id, student) → 允许同一人重复抢，放大超卖
--   无事务、无行锁、无乐观锁        → 由应用层「先读后改」制造竞态
--
-- 实验结束后这两张表保留在库里当对照组底账（D13 写报告要引用原始数据），
-- 不要 drop。清理只需 DELETE 数据。
--
-- 用法：随 `deploy/sql/` 整个目录挂到 MySQL 的 /docker-entrypoint-initdb.d 自动执行。
-- 本文件自包含（自带 CREATE TABLE 与 USE），与 schema.sql 无依赖关系，
-- 字母序排在 schema.sql 之前执行也没有影响。
-- 已建好库、只想补这两张表时：
--   docker exec -i cs-mysql mysql -uroot -pcampusslot < deploy/sql/baseline.sql
-- ============================================================

USE campusslot;

CREATE TABLE IF NOT EXISTS bare_activity (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  title       VARCHAR(200) NOT NULL DEFAULT 'D1 对照组活动',
  total_quota INT          NOT NULL COMMENT '总名额。故意不加 CHECK',
  remain      INT          NOT NULL COMMENT '剩余名额。故意允许为负 —— 负值就是超卖证据',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='D1 对照组：无约束活动表（不要加约束）';

CREATE TABLE IF NOT EXISTS bare_slot_record (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  activity_id BIGINT UNSIGNED NOT NULL,
  student_id  VARCHAR(32) NOT NULL,
  created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_bare_record_activity (activity_id)
  -- 注意：这里没有 UNIQUE KEY。加了就测不出超卖，见文件头说明。
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='D1 对照组：无唯一索引的申领记录';

-- 初始数据：200 个名额，对应需求书 §8.1「1000 个学号并发抢 200 个名额」
-- 这个数量选 200 而不是 3000，是因为超卖在 200 并发下就能稳定复现，
-- 不用等压到 8000 QPS。D5 才需要上量。
INSERT INTO bare_activity (id, title, total_quota, remain)
VALUES (1, 'D1 对照组：讲座入场 200 名额', 200, 200)
ON DUPLICATE KEY UPDATE total_quota = 200, remain = 200;

-- 重置（每轮实验前执行，接口 /api/baseline/reset 做的是同一件事）
-- DELETE FROM bare_slot_record WHERE activity_id = 1;
-- UPDATE bare_activity SET remain = total_quota WHERE id = 1;

-- 实验后的判据（remain < 0 才是真超卖，超卖数 = -remain）
-- 注意：records 与 consumed 恒等（成功一次 = 减一 + 插一条成对执行），
--       它们是同一个量的两种算法，不是两个独立校验。
-- SELECT COUNT(*)                                   AS records  FROM bare_slot_record WHERE activity_id = 1;
-- SELECT total_quota, remain                        FROM bare_activity WHERE id = 1;
-- 判据：remain < 0  →  超卖 = -remain = records - total_quota
