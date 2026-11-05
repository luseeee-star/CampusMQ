-- CampusSlot schema（需求规格说明书 v2.0）
-- 字符集统一 utf8mb4；所有时间字段由应用侧或 NOW() 写入，容器 TZ 必须 Asia/Shanghai。
-- 注意：MySQL 8.0.16+ 才真正强制 CHECK 约束，8.0.36 满足。
--
-- 【v2.0 修订】相对上一版的差异，改动理由都写在对应位置：
--   1. 删除 quota_lottery_entry / quota_lottery_result —— §9 明确不做抽签、摇号
--   2. 删除 credit_flow —— 学分/志愿时长不在 §2 的四类发放场景内
--   3. quota_resource 去掉 mode / draw_at，新增 cancel_deadline（S-4 要求有取消截止时间）
--   4. quota_record 去掉 REDEEMED 状态与 redeem_code —— §9 明确现场核销由一卡通承担
--   5. 新增 quota_college —— G-1 要求「按学院设配额」
--   6. 新增 quota_report —— G-4 / R-7 要求每次发放产出一份可归档的对账报表
--   7. notification 新增 next_retry_at —— SYS-2 要求通知失败可重试（不能只记 retry_count 而不排下次时间）

CREATE DATABASE IF NOT EXISTS campusslot
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

USE campusslot;

-- ============================================================
-- 部门（租户）
-- ============================================================
CREATE TABLE IF NOT EXISTS dept (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  code        VARCHAR(64)  NOT NULL COMMENT '部门编码',
  name        VARCHAR(128) NOT NULL COMMENT '部门名称',
  contact     VARCHAR(64)  DEFAULT NULL COMMENT '联系人',
  status      TINYINT      NOT NULL DEFAULT 1 COMMENT '1 启用 0 停用',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_dept_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='部门（租户）';

-- ============================================================
-- 资源定义（一场发放）
-- remain 是绝对防线的载体：UPDATE ... WHERE remain > 0
-- ============================================================
CREATE TABLE IF NOT EXISTS quota_resource (
  id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  dept_id             BIGINT UNSIGNED NOT NULL,
  title               VARCHAR(200) NOT NULL,
  description         VARCHAR(1000) DEFAULT NULL,
  total_quota         INT          NOT NULL COMMENT '总名额（整场发放的上限）',
  remain              INT          NOT NULL COMMENT '剩余名额，DB 侧最后一道防线',
  status              VARCHAR(16)  NOT NULL DEFAULT 'DRAFT' COMMENT 'DRAFT/PUBLISHED/CLOSED/OFFLINE',
  open_at             DATETIME     NOT NULL COMMENT '开放时刻，早于它一律拒绝（§4.2）',
  close_at            DATETIME     NOT NULL COMMENT '关闭时刻',
  cancel_deadline     DATETIME     DEFAULT NULL COMMENT 'S-4 取消截止时刻；到点后学生不可再主动取消',
  confirm_timeout_sec INT          NOT NULL DEFAULT 1800 COMMENT '锁定后确认时限，默认 30 分钟',
  roster_required     TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1 = 只允许资格名单内的学号（P4）',
  version             INT          NOT NULL DEFAULT 0,
  created_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_dept_status (dept_id, status),
  KEY idx_status_open (status, open_at),
  CONSTRAINT ck_resource_quota CHECK (total_quota >= 0),
  CONSTRAINT ck_resource_remain CHECK (remain >= 0),
  CONSTRAINT ck_resource_remain_le_total CHECK (remain <= total_quota),
  CONSTRAINT ck_resource_time CHECK (close_at > open_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资源定义：名额、开放时刻、确认时限';

-- ============================================================
-- 学院配额（G-1「按学院设配额」）
-- remain 与 quota 的差额 = 该学院已占用数，必须 <= quota，
-- 否则「按学院分配」只是报表上的一个字段，拦不住任何东西。
-- 只有资源开启了学院配额才建行；没有行表示该学院不限额（或不参与）。
-- ============================================================
CREATE TABLE IF NOT EXISTS quota_college (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  resource_id  BIGINT UNSIGNED NOT NULL,
  college_code VARCHAR(32)  NOT NULL COMMENT '学院编码，与资格名单 extra 字段对齐',
  quota        INT          NOT NULL COMMENT '该学院配额上限',
  remain       INT          NOT NULL COMMENT '该学院剩余配额',
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_college_resource_code (resource_id, college_code),
  CONSTRAINT ck_college_quota CHECK (quota >= 0),
  CONSTRAINT ck_college_remain CHECK (remain >= 0),
  CONSTRAINT ck_college_remain_le_quota CHECK (remain <= quota)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='学院配额';

-- ============================================================
-- 申领记录
-- (resource_id, student_id) 唯一索引 = 幂等最终防线（R-2 一人一条），
-- 重复投递 / 重试 / 狂点都靠它兜住
-- ============================================================
CREATE TABLE IF NOT EXISTS quota_record (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  resource_id  BIGINT UNSIGNED NOT NULL,
  dept_id      BIGINT UNSIGNED NOT NULL,
  student_id   VARCHAR(32)  NOT NULL,
  status       VARCHAR(16)  NOT NULL DEFAULT 'LOCKED' COMMENT 'LOCKED(已锁定)/CONFIRMED(已确认)/RELEASED(已释放)',
  source       VARCHAR(16)  NOT NULL DEFAULT 'FCFS' COMMENT 'FCFS 抢号 / BACKFILL 补位（R-6 定向补发）',
  -- 本表 6 个时间列统一毫秒精度（2026-11-04）。两个理由：
  --   ① claim_at / expire_at 是唯一参与「过期边界」判定的列，sweeper 用 expire_at <= now、
  --      confirm 用 expire_at > now，秒精度下 ±0.99s 的边界测不出来；
  --   ② 这 6 列互相做差：confirmed_at/released_at/updated_at 与 claim_at 是同一条
  --      UPDATE 里写的、created_at 与 claim_at 是同一条 INSERT 里写的。
  --      只升一端，差值就被精度低的那一端拉回秒级——「只升一端等于没升」。
  claim_at     DATETIME(3)  NOT NULL,
  expire_at    DATETIME(3)  NOT NULL COMMENT '确认截止时刻 = claim_at + confirm_timeout_sec',
  confirmed_at DATETIME(3)  DEFAULT NULL,
  released_at  DATETIME(3)  DEFAULT NULL,
  release_reason VARCHAR(32) DEFAULT NULL COMMENT 'TIMEOUT(超时) / CANCEL(主动取消) / SYSTEM',
  version      INT          NOT NULL DEFAULT 0,
  created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_record_resource_student (resource_id, student_id),
  KEY idx_record_student (student_id),
  KEY idx_record_status_expire (status, expire_at) COMMENT 'SYS-1 扫表用；quota_release_delay 定时消息尚未接入',
  CONSTRAINT ck_record_status CHECK (status IN ('LOCKED', 'CONFIRMED', 'RELEASED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='申领记录';

-- ============================================================
-- 资格名单（G-2）
-- 合规要求：只存学号与必要字段，不存身份证/手机号
-- ============================================================
CREATE TABLE IF NOT EXISTS quota_roster (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  resource_id BIGINT UNSIGNED NOT NULL,
  student_id  VARCHAR(32) NOT NULL,
  college_code VARCHAR(32) DEFAULT NULL COMMENT '学院编码，用于匹配 quota_college',
  extra       VARCHAR(255) DEFAULT NULL COMMENT '仅必要字段',
  created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_roster_resource_student (resource_id, student_id),
  KEY idx_roster_resource_college (resource_id, college_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资格名单';

-- ============================================================
-- 补位队列（R-6：释放的名额进队列定向通知，不重新开放抢号）
-- 重新开放抢号会制造第二次流量冲击，这是明确禁止的
-- ============================================================
CREATE TABLE IF NOT EXISTS quota_backfill_queue (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  resource_id BIGINT UNSIGNED NOT NULL,
  student_id  VARCHAR(32) NOT NULL,
  seq         BIGINT      NOT NULL COMMENT '入队序号，保证按序定向补发',
  status      VARCHAR(16) NOT NULL DEFAULT 'WAITING' COMMENT 'WAITING/NOTIFIED/EXPIRED/GRANTED',
  notified_at DATETIME    DEFAULT NULL,
  expire_at   DATETIME    DEFAULT NULL COMMENT '定向通知的有效期，过期让位给下一位',
  created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_backfill_resource_student (resource_id, student_id),
  KEY idx_backfill_resource_seq (resource_id, seq),
  KEY idx_backfill_status (status, expire_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='补位队列';

-- ============================================================
-- 通知记录（异步写，SYS-2）
-- 失败要能重试：只有 retry_count 而没有 next_retry_at，
-- 重试器就得全表扫，最后一定会变成「要么不重试，要么重试风暴」
-- ============================================================
CREATE TABLE IF NOT EXISTS notification (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  biz_id        VARCHAR(128) NOT NULL COMMENT '业务唯一键 = 事件:资源:学号，如 lock:3:S2024001（见 NotifyEvent.bizId）',
  student_id    VARCHAR(32) NOT NULL,
  resource_id   BIGINT UNSIGNED DEFAULT NULL,
  channel       VARCHAR(16)  NOT NULL COMMENT 'SMS/WECOM/MOCK',
  title         VARCHAR(200) DEFAULT NULL,
  content       VARCHAR(1000) DEFAULT NULL,
  status        VARCHAR(16)  NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING/SENT/FAILED/DEAD',
  retry_count   INT          NOT NULL DEFAULT 0,
  -- created_at / next_retry_at / sent_at 三列精度必须一致（2026-11-04）：
  -- 退避与送达时延都是「sent_at − created_at」或「next_retry_at − now」的差值，
  -- 只升一端会让差值退回到秒级，等于没升。
  next_retry_at DATETIME(3)  DEFAULT NULL COMMENT '下次重试时刻，指数退避',
  error_msg     VARCHAR(500) DEFAULT NULL,
  sent_at       DATETIME(3)  DEFAULT NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_notify_biz_channel (biz_id, channel),
  -- (resource_id, student_id) 是 D9 报表与 quota_record 关联的键。
  -- 注意 resource_id 可空，NULL 行不进这个索引——当前所有写点都填了 resource_id。
  KEY idx_notify_resource_student (resource_id, student_id),
  -- 只有 (status, next_retry_at) 一个状态索引：单独 (status) 是它的最左前缀，
  -- 多一个只多一次索引写（2026-11-04 删除了原来的 idx_notify_status）。
  KEY idx_notify_retry (status, next_retry_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='通知记录';

-- ============================================================
-- 接口级幂等日志
-- 与 Redis 幂等键的关系：Redis 是快路径，这张表是「Redis 挂了也能查到首次结果」的底账
-- ============================================================
CREATE TABLE IF NOT EXISTS idempotent_log (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  idempotent_key VARCHAR(160) NOT NULL,
  student_id     VARCHAR(32) NOT NULL,
  resource_id    BIGINT UNSIGNED NOT NULL,
  result_code    VARCHAR(32) DEFAULT NULL COMMENT '首次请求的结果码，用于 R-3 原样返回',
  record_id      BIGINT UNSIGNED DEFAULT NULL,
  created_at     DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_idem_key (idempotent_key),
  KEY idx_idem_student_resource (student_id, resource_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='接口级幂等日志';

-- ============================================================
-- 对账差异（SYS-3 每 5 分钟巡检 + S-4 释放拒绝事件）
-- 分布式系统里「永远一致」是幻想，可检测 + 可修复才是目标
--
-- 两类记录共用一张表，用 source 区分：
--   SCAN    —— 巡检按资源聚合产生（student_id / record_id 为 NULL）
--   CANCEL  —— 取消时 release.lua 返回 -1/-2（DB 已提交 +1，Redis 没涨）
--   TIMEOUT —— 超时释放时同上
-- 共用一张表的理由：两者的"修复动作"完全相同（把 Redis remain 对齐到 DB），
-- D9 只需扫一个「未修队列」（fixed_at IS NULL），不必 union 两张表。
-- 事件级记录必须带 student_id / source，否则 D9 只知道"差 1"不知道为什么。
-- ============================================================
CREATE TABLE IF NOT EXISTS reconcile_diff (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  resource_id  BIGINT UNSIGNED NOT NULL,
  redis_remain INT          NOT NULL,
  db_remain    INT          NOT NULL,
  drift        INT          NOT NULL COMMENT 'redis_remain - db_remain',
  fix_action   VARCHAR(255) DEFAULT NULL,
  detected_at  DATETIME(3)  NOT NULL,
  fixed_at     DATETIME(3)  DEFAULT NULL COMMENT 'NULL = 未修复，D9 修完回填',
  source       VARCHAR(16)  NOT NULL DEFAULT 'SCAN' COMMENT 'SCAN / CANCEL / TIMEOUT',
  student_id   VARCHAR(32)  DEFAULT NULL COMMENT '事件级记录才有',
  record_id    BIGINT UNSIGNED DEFAULT NULL COMMENT '事件级记录才有',
  total_quota  INT          DEFAULT NULL COMMENT '判定时的配额上限；扩容后旧记录可能不等于当前值',
  PRIMARY KEY (id),
  KEY idx_diff_resource_detected (resource_id, detected_at),
  KEY idx_diff_unfixed (fixed_at),
  KEY idx_diff_source_detected (source, detected_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账差异';

-- ============================================================
-- 对账报表（G-4 / R-7：每次发放必须产出一份可归档的报表）
-- 平衡等式：applied = confirmed + released + locked(尚未确认)
-- 只算不存的话，活动结束三天后再被问「当时发了多少」，没人答得出来
-- ============================================================
CREATE TABLE IF NOT EXISTS quota_report (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  resource_id   BIGINT UNSIGNED NOT NULL,
  dept_id       BIGINT UNSIGNED NOT NULL,
  total_quota   INT          NOT NULL,
  applied_cnt   INT          NOT NULL COMMENT '申请总数（含已释放）',
  granted_cnt   INT          NOT NULL COMMENT '成功锁定数',
  confirmed_cnt INT          NOT NULL COMMENT '已确认数',
  released_cnt  INT          NOT NULL COMMENT '已释放数',
  locked_cnt    INT          NOT NULL COMMENT '仍处于锁定中的数量',
  remain        INT          NOT NULL COMMENT '剩余名额',
  balanced      TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '平衡等式是否成立',
  generated_at  DATETIME     NOT NULL COMMENT '报表生成时刻',
  PRIMARY KEY (id),
  KEY idx_report_resource (resource_id, generated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每次发放的对账报表';

-- ============================================================
-- 增量变更（对已存在的库执行；上面的 CREATE TABLE IF NOT EXISTS 不会改已有表）
--
-- 2026-11-04  reconcile_diff 支持事件级对账（S-4）
--   S-4 取消/超时释放时，release.lua 返回 -1/-2 表示 Redis 侧拒绝回补
--   （key 不存在 / remain 已到 total_quota 上界），而 DB 事务已经提交了 +1。
--   原来的表只有按资源聚合的 7 列，装不下"是哪个学生、哪次取消、为什么拒绝"。
-- ============================================================
-- ALTER TABLE reconcile_diff
--   ADD COLUMN source      VARCHAR(16) NOT NULL DEFAULT 'SCAN' COMMENT 'SCAN / CANCEL / TIMEOUT' AFTER fixed_at,
--   ADD COLUMN student_id  VARCHAR(32)     DEFAULT NULL COMMENT '事件级记录才有' AFTER source,
--   ADD COLUMN record_id   BIGINT UNSIGNED DEFAULT NULL COMMENT '事件级记录才有' AFTER student_id,
--   ADD COLUMN total_quota INT             DEFAULT NULL COMMENT '判定时的配额上限' AFTER record_id,
--   ADD KEY idx_diff_source_detected (source, detected_at);
-- 存量记录由 DEFAULT 'SCAN' 自动归类，不需要 UPDATE 回填。
--
-- 2026-11-04  SYS-2 通知重试落地
--   notification 表结构不变（不加 record_id：quota_record 的 uk_record_resource_student
--   让 (resource_id, student_id) 就是与主键一一对应的索引键，D9 关联是单行精确命中）。
--   只改了两处注释：biz_id 的示例从 lock:{recordId} 改成 事件:资源:学号；
--   quota_record.idx_record_status_expire 的注释去掉"主路径是 MQ 定时消息"（那是计划）。
--   注释同步到已存在的库：
-- ALTER TABLE notification MODIFY COLUMN biz_id VARCHAR(128) NOT NULL
--   COMMENT '业务唯一键 = 事件:资源:学号，如 lock:3:S2024001（见 NotifyEvent.bizId）';

-- 2026-11-04  时间列升到毫秒精度（11 列 / 3 表）
--   触发原因：D8 收尾发现「退避 1/2/4/8/16/32 秒」和「NFR-5 的 0.5s」两个数，
--   在 DATETIME(0) 下只能写到秒，边界实验测不出 ±1s 以内的差别。
--   范围只取「参与同一个比较或同一个差值」的列，不碰无关表：
--     quota_record    claim_at / expire_at / confirmed_at / released_at
--                     / created_at / updated_at
--                     —— expire_at <= now(scan) 与 > now(confirm) 的互补性；
--                        这 6 列都从同一条 INSERT 或同一条 UPDATE 里的同一个 #{now} 写出，
--                        只要有一列留在秒精度，对应的差值就被它拉回秒级
--                        （典型：updated_at − claim_at、created_at − claim_at 看起来会有 0.5s 的假偏移）
--     notification    created_at / next_retry_at / sent_at —— sent_at−created_at 是送达时延
--     reconcile_diff  detected_at / fixed_at   —— 修复时延
--   quota_resource.open_at/close_at、quota_college / quota_roster / quota_report 等表的
--   created_at/updated_at 保持 DATETIME(0)：它们没有亚秒比较，且 D10 之后是几十万行，
--   升精度只多一次表重建与页分裂。
--   注意：只升一端不升另一端等于没升——差值会被精度较低的那一端拉回秒级。
-- ============================================================
-- ALTER TABLE quota_record
--   MODIFY COLUMN claim_at     DATETIME(3) NOT NULL,
--   MODIFY COLUMN expire_at    DATETIME(3) NOT NULL COMMENT '确认截止时刻 = claim_at + confirm_timeout_sec',
--   MODIFY COLUMN confirmed_at DATETIME(3) DEFAULT NULL,
--   MODIFY COLUMN released_at  DATETIME(3) DEFAULT NULL,
--   MODIFY COLUMN created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
--   MODIFY COLUMN updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3);
-- ALTER TABLE notification
--   MODIFY COLUMN next_retry_at DATETIME(3) DEFAULT NULL COMMENT '下次重试时刻，指数退避',
--   MODIFY COLUMN sent_at       DATETIME(3) DEFAULT NULL,
--   MODIFY COLUMN created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3);
-- ALTER TABLE reconcile_diff
--   MODIFY COLUMN detected_at DATETIME(3) NOT NULL,
--   MODIFY COLUMN fixed_at    DATETIME(3) DEFAULT NULL COMMENT 'NULL = 未修复，D9 修完回填';
-- 存量数据自动补 .000，不需要回填。
