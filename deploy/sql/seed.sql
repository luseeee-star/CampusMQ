-- 最小可用种子数据：两个部门、三场发放、一批资格名单、一场带学院配额。
-- 3.2 万学生规模的压测数据由 benchmark/seed-data.sql 生成（D2 产出），不放在这里，
-- 避免每次 reset 都灌 3 万行拖慢基线恢复。
--
-- 三场活动对应需求书 §2 的三种流量形态，压测时可以直接拿来做对照：
--   id=1  讲座入场      200 名额，开放后 5 秒打满（S1 单资源抢号）
--   id=2  团操课         80 名额，带学院配额（验证 quota_college 真拦得住）
--   id=3  迎新物资     3000 名额，瞬时冲击最大（S3 大名额 + 高 QPS）

USE campusslot;

INSERT INTO dept (id, code, name, contact, status) VALUES
  (1, 'XSC', '学生工作处', '王老师', 1),
  (2, 'TYB', '体育部',     '李老师', 1)
ON DUPLICATE KEY UPDATE name = VALUES(name);

-- 开放时刻统一设为「当前时间 - 1 分钟」，起完环境就能直接压，不用等整点
INSERT INTO quota_resource
  (id, dept_id, title, description, total_quota, remain, status,
   open_at, close_at, cancel_deadline, confirm_timeout_sec, roster_required)
VALUES
  (1, 1, '《名家讲坛》第 3 期入场名额', '先到先得，余量为 0 即止',
   200, 200, 'PUBLISHED',
   NOW() - INTERVAL 1 MINUTE, NOW() + INTERVAL 7 DAY, NOW() + INTERVAL 7 DAY, 1800, 0),
  (2, 2, '秋季团操课（按学院配额）', '每个学院 20 个名额，名额满即止',
   80, 80, 'PUBLISHED',
   NOW() - INTERVAL 1 MINUTE, NOW() + INTERVAL 3 DAY, NOW() + INTERVAL 3 DAY, 1800, 1),
  (3, 1, '迎新物资领取（雨伞 + 帆布袋）', '瞬时冲击最大的场景，3000 名额',
   3000, 3000, 'PUBLISHED',
   NOW() - INTERVAL 1 MINUTE, NOW() + INTERVAL 7 DAY, NOW() + INTERVAL 7 DAY, 1800, 0)
ON DUPLICATE KEY UPDATE
  total_quota = VALUES(total_quota), remain = VALUES(remain),
  open_at = VALUES(open_at), close_at = VALUES(close_at),
  cancel_deadline = VALUES(cancel_deadline),
  confirm_timeout_sec = VALUES(confirm_timeout_sec),
  roster_required = VALUES(roster_required);

-- 学院配额：只有 id=2 这场开了配额，四个学院各 20 = 80，正好等于总名额
INSERT INTO quota_college (resource_id, college_code, quota, remain) VALUES
  (2, 'CS', 20, 20),
  (2, 'EE', 20, 20),
  (2, 'ME', 20, 20),
  (2, 'ART', 20, 20)
ON DUPLICATE KEY UPDATE quota = VALUES(quota), remain = VALUES(remain);

-- 资格名单：200 个学号，平均分到四个学院（CS/EE/ME/ART 各 50）
-- id=2 这场 roster_required=1，名单外的学号必须被拦在 1005 NOT_ELIGIBLE
INSERT INTO quota_roster (resource_id, student_id, college_code, extra)
SELECT 2,
       CONCAT('2023', LPAD(seq, 6, '0')),
       ELT(1 + (seq MOD 4), 'CS', 'EE', 'ME', 'ART'),
       'seed'
FROM (
  SELECT (a.n + b.n * 10 + c.n * 100 + 1) AS seq
  FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
  CROSS JOIN (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b
  CROSS JOIN (SELECT 0 n UNION SELECT 1) c
) t
WHERE seq <= 200
ON DUPLICATE KEY UPDATE college_code = VALUES(college_code);
