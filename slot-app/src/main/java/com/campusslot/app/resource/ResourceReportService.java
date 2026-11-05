package com.campusslot.app.resource;

import com.campusslot.app.resource.dto.CollegeProgress;
import com.campusslot.app.resource.dto.ProgressView;
import com.campusslot.common.api.BizException;
import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.RedisKeys;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 发放进度与对账导出（G-3、G-4）。
 *
 * <p>只读。这里不写任何一张表——报表一旦能改数据，"看到的数"和"改后的数"就分不清了。
 *
 * <p><b>三套账为什么必须并列</b>：见 {@link ProgressView} 的类注释。一句话是
 * {@code dbRemain} 滞后、{@code redisRemain} 是抢占那一刻的真相、{@code expectRemain}
 * 由记录反推。只报一个数，看的人分不清"发完了"和"账坏了"。
 *
 * <p><b>学院维度有两个来源，绝不能 JOIN 成一个数</b>：
 * <ul>
 *   <li>实际领取 = {@code quota_record LEFT JOIN quota_roster}，join 不上的是「未分学院」桶；</li>
 *   <li>配置限额 = {@code quota_college}，是"最多给几个"，和实际领走多少没有推导关系。</li>
 * </ul>
 * 两张表没有任何一条可靠的外键能保证一一对应（{@code quota_college} 可以不覆盖所有学院，
 * 名单也可能写了配额里没有的编码），硬 JOIN 会丢掉行、造出第三个谁也不是的数字。
 *
 * <p><b>学院维度是"名单上写的"，不是"发的那一刻扣的"</b>：{@code quota_record} 里
 * 压根没有 {@code college_code} 这一列，只有 {@code dept_id}（部门）。所以任何按学院的
 * 报数都必须在报告里交代来源，否则读的人会以为是扣减那一刻的学院——两者在
 * "事后改过名单"的情况下会不一致。
 */
@Service
public class ResourceReportService {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;

    public ResourceReportService(JdbcTemplate jdbc, StringRedisTemplate redis) {
        this.jdbc = jdbc;
        this.redis = redis;
    }

    // ------------------------------------------------------------------
    // 进度
    // ------------------------------------------------------------------

    /**
     * @param deptId 可选的部门过滤。一个活动只属于一个部门，所以传了就必须和活动一致——
     *               不一致直接报错，而不是返回一堆 0。返回 0 会让人以为"这个部门真的没人领"，
     *               而那其实是参数打错了。
     */
    public ProgressView progress(long resourceId, Long deptId) {
        Meta meta = loadMeta(resourceId);
        if (deptId != null && deptId != meta.deptId()) {
            throw new BizException(ErrorCode.PARAM_INVALID,
                    "deptId 与活动所属部门不一致：活动 " + resourceId + " 属于 " + meta.deptId() + "，请求 " + deptId);
        }

        Counts c = loadCounts(resourceId, deptId);
        int expectRemain = meta.totalQuota() - (c.locked() + c.confirmed());
        int dbRemain = meta.remain();

        String stockRaw = redis.opsForValue().get(RedisKeys.stock(resourceId));
        Integer redisRemain = stockRaw == null ? null : Integer.valueOf(stockRaw);

        Map<String, CollegeRow> configured = loadConfiguredColleges(resourceId);
        Map<String, ActualCell> actual = loadActualByCollege(resourceId, deptId);
        Map<String, Integer> rosterByCollege = loadRosterByCollege(resourceId);

        List<CollegeProgress> colleges = assembleColleges(configured, actual, rosterByCollege, resourceId);

        int rosterRows = rosterByCollege.values().stream().mapToInt(Integer::intValue).sum();
        // 名单 key 数只能去 Redis 数：行数在 DB 里，key 在 Redis 里，
        // 而"灌漏了"这件事只有两者对比才看得出来（漏灌是静默的：学生被判 1005，不报错）
        int rosterKeys = meta.rosterRequired() ? countKeys(RedisKeys.rosterEntryPrefix(resourceId) + "*") : 0;

        return new ProgressView(
                resourceId, meta.title(), meta.status(), meta.deptId(), meta.rosterRequired(),
                meta.openAt(), meta.closeAt(),
                meta.totalQuota(), dbRemain, redisRemain,
                stockRaw == null,
                expectRemain,
                redisRemain == null ? null : redisRemain - dbRemain,
                dbRemain == expectRemain,
                redisRemain != null && redisRemain == dbRemain,
                c.locked(), c.confirmed(), c.released(), c.releasedByTimeout(), c.releasedByCancel(),
                rosterRows, rosterKeys, configured.size(),
                colleges,
                AppTime.now());
    }

    // ------------------------------------------------------------------
    // 导出
    // ------------------------------------------------------------------

    /**
     * 对账 CSV。默认按学院分组，{@code groupBy=none} 只出合计。
     *
     * <p>文件头带 BOM：不加的话 Excel 打开中文列名是乱码（Windows 版按 GBK 猜编码），
     * 而对账表的第一读者就是人。
     *
     * <p>「未分学院」那一行不会被 {@code GROUP BY} 丢掉——它不是空值噪音，是真实的桶：
     * {@code roster_required = 0} 的活动（如 r1、r3）整份记录都 join 不上名单，
     * 手工插入的记录也 join 不上。合计行必须等于各学院之和，否则看起来像丢了几条。
     */
    public String exportCsv(long resourceId, Long deptId, String groupBy) {
        ProgressView v = progress(resourceId, deptId);
        boolean byCollege = !"none".equalsIgnoreCase(groupBy == null ? "college" : groupBy);

        StringBuilder sb = new StringBuilder(2048);
        sb.append('\ufeff');

        // 注释行用 '#' 开头：对账表是给人看的，口径必须跟着文件走，
        // 否则文件传到别人手里就只剩数字，没人知道「实际领取」是怎么分组的
        line(sb, "# CampusSlot 对账导出");
        line(sb, "# 活动 " + v.resourceId() + "「" + nz(v.title()) + "」 状态 " + v.status() + " 部门 " + v.deptId());
        line(sb, "# 导出时间 " + TS.format(v.now()));
        line(sb, "# 总名额 " + v.totalQuota() + " / DB 余量 " + v.dbRemain()
                + " / Redis 余量 " + (v.stockKeyMissing() ? "缺失(key 不存在)" : v.redisRemain())
                + " / 已锁定 " + v.locked() + " / 已确认 " + v.confirmed()
                + " / 已释放 " + v.released() + "（超时 " + v.releasedByTimeout() + "、取消 " + v.releasedByCancel() + "）");
        line(sb, "# 名单 " + v.rosterRows() + " 行 / Redis key " + v.rosterKeys() + " 个"
                + (v.rosterRequired() && v.rosterRows() != v.rosterKeys() ? "  ← 不相等，说明名单漏灌" : ""));
        line(sb, "# 口径一（配置）：quota 来自 quota_college，是「最多给几个」");
        line(sb, "# 口径二（实际）：locked/confirmed/issued/released 来自 quota_record，是「实际领走几个」");
        line(sb, "#   按学院分组只能 LEFT JOIN quota_roster（quota_record 没有 college_code 列），");
        line(sb, "#   名单外的记录落在「未分学院」桶，它是真实存在的一行，合计包含它");
        line(sb, "#   学院是「名单上写的」，不是「扣减那一刻的」");
        line(sb, "# drift = remain_redis - remain_db；空表示 Redis key 不存在");
        line(sb, "");

        sb.append("college_code,college_label,configured,quota,remain_db,remain_redis,drift,")
                .append("roster_rows,locked,confirmed,issued,released").append('\n');

        int sumQuota = 0, sumIssued = 0, sumLocked = 0, sumConfirmed = 0, sumReleased = 0, sumRoster = 0;
        for (CollegeProgress cp : v.colleges()) {
            if (byCollege) {
                row(sb, cp);
            }
            sumQuota += cp.quota() == null ? 0 : cp.quota();
            sumIssued += cp.issued();
            sumLocked += cp.locked();
            sumConfirmed += cp.confirmed();
            sumReleased += cp.released();
            sumRoster += cp.rosterRows();
        }

        // 合计行独立算一遍，不直接抄 v：两边不等就说明组装学院时漏了行（比如某个编码两边都没出现）
        sb.append("TOTAL,合计,,")
                .append(sumQuota).append(',')
                .append(v.dbRemain()).append(',')
                .append(v.stockKeyMissing() ? "" : String.valueOf(v.redisRemain())).append(',')
                .append(v.drift() == null ? "" : String.valueOf(v.drift())).append(',')
                .append(sumRoster).append(',')
                .append(sumLocked).append(',')
                .append(sumConfirmed).append(',')
                .append(sumIssued).append(',')
                .append(sumReleased).append('\n');

        line(sb, "");
        line(sb, "# 合计校验：各学院 issued 之和 = " + sumIssued
                + "，活动总计 issued = " + (v.locked() + v.confirmed())
                + (sumIssued == v.locked() + v.confirmed() ? "，一致" : "，**不一致**"));
        line(sb, "# 配置限额之和 = " + sumQuota + "，总名额 = " + v.totalQuota()
                + (sumQuota <= v.totalQuota() ? "" : "，**超过总名额**"));
        return sb.toString();
    }

    private void row(StringBuilder sb, CollegeProgress cp) {
        sb.append(csv(cp.collegeCode())).append(',')
                .append(csv(cp.collegeLabel())).append(',')
                .append(cp.configured() ? "1" : "0").append(',')
                .append(cp.quota() == null ? "" : String.valueOf(cp.quota())).append(',')
                .append(cp.remainDb() == null ? "" : String.valueOf(cp.remainDb())).append(',')
                .append(cp.remainRedis() == null ? "" : String.valueOf(cp.remainRedis())).append(',')
                .append(cp.drift() == null ? "" : String.valueOf(cp.drift())).append(',')
                .append(cp.rosterRows()).append(',')
                .append(cp.locked()).append(',')
                .append(cp.confirmed()).append(',')
                .append(cp.issued()).append(',')
                .append(cp.released()).append('\n');
    }

    private static void line(StringBuilder sb, String s) {
        sb.append(s).append('\n');
    }

    /** CSV 字段转义：含逗号、引号、换行时整体加引号，内部引号翻倍。 */
    private static String csv(String s) {
        if (s == null) {
            return "";
        }
        if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0) {
            return s;
        }
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    // ------------------------------------------------------------------
    // 取数
    // ------------------------------------------------------------------

    private record Meta(long id, String title, String status, long deptId, boolean rosterRequired,
                        int totalQuota, int remain, LocalDateTime openAt, LocalDateTime closeAt) {
    }

    private Meta loadMeta(long resourceId) {
        Meta m = jdbc.query(
                "SELECT id, title, status, dept_id, roster_required, total_quota, remain, open_at, close_at "
                        + "FROM quota_resource WHERE id = ?",
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    return new Meta(
                            rs.getLong("id"), rs.getString("title"), rs.getString("status"),
                            rs.getLong("dept_id"), rs.getBoolean("roster_required"),
                            rs.getInt("total_quota"), rs.getInt("remain"),
                            rs.getTimestamp("open_at") == null ? null : rs.getTimestamp("open_at").toLocalDateTime(),
                            rs.getTimestamp("close_at") == null ? null : rs.getTimestamp("close_at").toLocalDateTime());
                },
                resourceId);
        if (m == null) {
            throw new BizException(ErrorCode.RESOURCE_NOT_FOUND);
        }
        return m;
    }

    private record Counts(int locked, int confirmed, int released, int releasedByTimeout, int releasedByCancel) {
    }

    private Counts loadCounts(long resourceId, Long deptId) {
        // COALESCE 不能省：活动一条记录都没有时 SUM 返回 NULL，直接拆箱会 NPE
        String sql = """
                SELECT COALESCE(SUM(status = 'LOCKED'), 0)                                  AS locked,
                       COALESCE(SUM(status = 'CONFIRMED'), 0)                               AS confirmed,
                       COALESCE(SUM(status = 'RELEASED'), 0)                                AS released,
                       COALESCE(SUM(status = 'RELEASED' AND release_reason = 'TIMEOUT'), 0) AS rel_timeout,
                       COALESCE(SUM(status = 'RELEASED' AND release_reason = 'CANCEL'), 0)  AS rel_cancel
                  FROM quota_record
                 WHERE resource_id = ?
                """;
        Object[] args = deptId == null ? new Object[]{resourceId} : new Object[]{resourceId, deptId};
        String q = deptId == null ? sql : sql + " AND dept_id = ?";
        return jdbc.queryForObject(q, (rs, i) -> new Counts(
                rs.getInt("locked"), rs.getInt("confirmed"), rs.getInt("released"),
                rs.getInt("rel_timeout"), rs.getInt("rel_cancel")), args);
    }

    /** 一个学院在组装过程中的累加器；配置口径和实际口径各自填自己的字段。 */
    private static final class CollegeRow {
        Integer quota;
        Integer remainDb;
        boolean configured;
    }

    private Map<String, CollegeRow> loadConfiguredColleges(long resourceId) {
        // LinkedHashMap 而不是 TreeMap：下面还有一张表可能带 null 键（「未分学院」），
        // 而 TreeMap 一 put null 就 NPE。排序统一放到组装那一步做。
        Map<String, CollegeRow> map = new LinkedHashMap<>();
        jdbc.query("SELECT college_code, quota, remain FROM quota_college WHERE resource_id = ?",
                rs -> {
                    CollegeRow row = new CollegeRow();
                    row.configured = true;
                    row.quota = rs.getInt("quota");
                    row.remainDb = rs.getInt("remain");
                    map.put(rs.getString("college_code"), row);
                },
                resourceId);
        return map;
    }

    private record ActualCell(int locked, int confirmed, int released) {
    }

    private Map<String, ActualCell> loadActualByCollege(long resourceId, Long deptId) {
        Map<String, ActualCell> map = new LinkedHashMap<>();
        String base = """
                SELECT r.college_code AS code,
                       COALESCE(SUM(q.status = 'LOCKED'), 0)    AS locked,
                       COALESCE(SUM(q.status = 'CONFIRMED'), 0) AS confirmed,
                       COALESCE(SUM(q.status = 'RELEASED'), 0)  AS released
                  FROM quota_record q
                  LEFT JOIN quota_roster r
                         ON r.resource_id = q.resource_id AND r.student_id = q.student_id
                 WHERE q.resource_id = ?
                """;
        Object[] args = deptId == null ? new Object[]{resourceId} : new Object[]{resourceId, deptId};
        String q = deptId == null ? base : base + " AND q.dept_id = ?";
        jdbc.query(q + " GROUP BY r.college_code", rs -> {
            map.put(rs.getString("code"), new ActualCell(
                    rs.getInt("locked"), rs.getInt("confirmed"), rs.getInt("released")));
        }, args);
        return map;
    }

    private Map<String, Integer> loadRosterByCollege(long resourceId) {
        // 同样不能是 TreeMap：手工 SQL 插进来的名单行 college_code 可以为 NULL
        Map<String, Integer> map = new LinkedHashMap<>();
        jdbc.query("SELECT college_code, COUNT(*) AS c FROM quota_roster WHERE resource_id = ? "
                        + "GROUP BY college_code",
                rs -> {
                    map.put(rs.getString("college_code"), rs.getInt("c"));
                },
                resourceId);
        return map;
    }

    /**
     * 把三份来源合成一行行学院。
     *
     * <p><b>编码集合取并集，不是交集</b>：只在 {@code quota_college} 里出现的（配了名额没人领）、
     * 只在 {@code quota_roster} 里出现的（名单写了这个学院、但没配限额——这是合法的）、
     * 只在 {@code quota_record} 里出现的（记录领了、名单里没有这个人），
     * 三种都必须在表里有一行。取交集会把它们全丢掉，表现为"各学院加起来比总数少"。
     */
    private List<CollegeProgress> assembleColleges(Map<String, CollegeRow> configured,
                                                   Map<String, ActualCell> actual,
                                                   Map<String, Integer> rosterByCollege,
                                                   long resourceId) {
        // 编码集合取并集。TreeSet 不接受 null 键，而「未分学院」正是 null —— 单独拆出来处理，
        // 用 addAll 整份塞进去会直接 NPE，而且是在"某个活动的名单 join 不上"时才炸，
        // 平时跑不到，最难查的那一类。
        TreeSet<String> codes = new TreeSet<>();
        addNonNull(codes, configured.keySet());
        addNonNull(codes, actual.keySet());
        addNonNull(codes, rosterByCollege.keySet());

        Map<String, String> collegeRemainRedis = readCollegeRemain(resourceId);

        List<CollegeProgress> out = new ArrayList<>(codes.size() + 1);
        for (String code : codes) {
            out.add(build(code, configured.get(code), actual.get(code),
                    rosterByCollege.getOrDefault(code, 0), collegeRemainRedis.get(code)));
        }
        // 「未分学院」只要是三份来源里任意一份出现过，就必须有一行
        if (configured.containsKey(null) || actual.containsKey(null) || rosterByCollege.containsKey(null)) {
            // 放在最后：这一桶通常是 0 条或极少数，夹在 CS/EE 中间会打断阅读
            out.add(build(null, configured.get(null), actual.get(null),
                    rosterByCollege.getOrDefault(null, 0), null));
        }
        return out;
    }

    private static void addNonNull(TreeSet<String> target, Set<String> src) {
        for (String s : src) {
            if (s != null) {
                target.add(s);
            }
        }
    }

    private CollegeProgress build(String code, CollegeRow cfg, ActualCell act, int rosterRows, String redisRaw) {
        Integer remainRedis = redisRaw == null ? null : Integer.valueOf(redisRaw);
        int locked = act == null ? 0 : act.locked();
        int confirmed = act == null ? 0 : act.confirmed();
        return new CollegeProgress(
                code,
                code == null ? "未分学院" : code,
                cfg != null && cfg.configured,
                cfg == null ? null : cfg.quota,
                cfg == null ? null : cfg.remainDb,
                remainRedis,
                remainRedis == null || cfg == null ? null : remainRedis - cfg.remainDb,
                rosterRows,
                locked, confirmed, locked + confirmed,
                act == null ? 0 : act.released());
    }

    /** 一次 pipeline 读掉所有学院余量，不用逐个 GET。 */
    private Map<String, String> readCollegeRemain(long resourceId) {
        Map<String, String> out = new LinkedHashMap<>();
        List<String> codes = jdbc.queryForList(
                "SELECT college_code FROM quota_college WHERE resource_id = ?", String.class, resourceId);
        if (codes.isEmpty()) {
            return out;
        }
        List<String> keys = new ArrayList<>(codes.size());
        for (String code : codes) {
            keys.add(RedisKeys.collegeStock(resourceId, code));
        }
        List<String> values = redis.opsForValue().multiGet(keys);
        for (int i = 0; i < codes.size(); i++) {
            String v = values == null ? null : values.get(i);
            if (v != null) {
                out.put(codes.get(i), v);
            }
        }
        return out;
    }

    /**
     * SCAN 数 key。用 SCAN 而不是 {@code KEYS}：{@code KEYS} 是 O(N) 阻塞命令，
     * 在压测跑着的库上执行会直接把 Redis 卡住，而这里恰好是"压测进行中也要能打开"的页面。
     *
     * <p>前提是单机 Redis。换成 Cluster 之后 SCAN 只覆盖当前分片，这个数会偏小——
     * 那时要么按节点逐个扫，要么在灌名单时维护一个计数 key。现在只有单机，不做这层。
     */
    private int countKeys(String pattern) {
        AtomicInteger n = new AtomicInteger();
        ScanOptions opts = ScanOptions.scanOptions().match(pattern).count(500).build();
        try (Cursor<String> c = redis.scan(opts)) {
            while (c.hasNext()) {
                c.next();
                n.incrementAndGet();
            }
        }
        return n.get();
    }
}
