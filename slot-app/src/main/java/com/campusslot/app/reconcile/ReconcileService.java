package com.campusslot.app.reconcile;

import com.campusslot.app.reconcile.dto.ReconcileOutcome;
import com.campusslot.common.api.BizException;
import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.RedisKeys;
import com.campusslot.common.entity.QuotaReport;
import com.campusslot.common.entity.ReconcileDiff;
import com.campusslot.common.mapper.QuotaReportMapper;
import com.campusslot.common.mapper.ReconcileDiffMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * 对账巡检的单活动实现（SYS-3 / NFR-4）。
 *
 * <p>口径只有一条，和 {@code deploy/sql/report.sql} 结尾那句一致：
 * <pre>
 *   total_quota − (LOCKED + CONFIRMED)  ==  quota_resource.remain  ==  quota:stock:{id}
 * </pre>
 * 左边是"由记录反推"，中间是"列上的原值"，右边是"抢占那一刻的真相"。
 * 三者相等才叫平；任何一处不等都要能定位是哪一处。
 *
 * <p><b>不按 count(*) 比、按 (total − 有效数) 比</b>：取消会让记录变成 RELEASED，
 * 累计行数只增不减，拿它比余量必然越跑越偏。而且删记录这个验收动作**不会动 {@code remain} 列**，
 * 用列去比差异恒为 0，看起来像"巡检根本没跑"。
 *
 * <p><b>为什么这里不修数据</b>：巡检只负责发现和留证。自动修的诱惑很大（"把 Redis 改成 DB 的值"
 * 一行代码），但 Redis 和 DB 谁对，取决于差异的方向和成因——在途消息时 Redis 是对的，
 * 记录被删时 DB 是对的，人工改过 Redis 时两边都不一定对。猜错了就是静默丢名额或者超发。
 * 所以 {@code fixed_at} 一律留空，由人确认后再填（{@code D9 修完回填} 就是这个约定）。
 */
@Service
public class ReconcileService {

    private static final Logger log = LoggerFactory.getLogger(ReconcileService.class);

    public static final String ACTION_NONE = "NONE";
    public static final String ACTION_DIFF = "DIFF";
    public static final String ACTION_STOCK_KEY_MISSING = "STOCK_KEY_MISSING";
    public static final String ACTION_REPORT_FINAL = "REPORT_FINAL";
    public static final String ACTION_SKIPPED = "SKIPPED";

    public static final String SEVERITY_INFO = "INFO";
    public static final String SEVERITY_WARN = "WARN";

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final ReconcileDiffMapper diffMapper;
    private final QuotaReportMapper reportMapper;
    private final long intervalMs;

    public ReconcileService(JdbcTemplate jdbc,
                            StringRedisTemplate redis,
                            ReconcileDiffMapper diffMapper,
                            QuotaReportMapper reportMapper,
                            @Value("${campusslot.reconcile.interval-ms:300000}") long intervalMs) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.diffMapper = diffMapper;
        this.reportMapper = reportMapper;
        this.intervalMs = intervalMs;
    }

    /**
     * 对一个已发布活动做一次比对；有差异就落 {@code reconcile_diff} 和 {@code quota_report}。
     *
     * <p>没有差异时<b>什么都不写</b>：{@code reconcile_diff} 记的是"出过问题"，
     * 每 5 分钟写一条 drift=0 会把真正的差异埋掉；{@code quota_report} 同理。
     */
    public ReconcileOutcome reconcile(long resourceId) {
        ResourceRow row = loadResource(resourceId);

        // 只有已发布的活动才有"可比的库存 key"。草稿还没有 key；已关闭/下架的活动
        // key 可能已经被清理，拿它对账只会得到一条必然出现的"key 缺失"——
        // 那种告警天天响，响到没人看为止。这两类走各自的出口。
        if ("DRAFT".equals(row.status())) {
            Counts skip = countRecords(resourceId);
            return new ReconcileOutcome(resourceId, row.status(), row.totalQuota(),
                    skip.records(), skip.locked(), skip.confirmed(), skip.released(),
                    skip.locked() + skip.confirmed(), row.totalQuota() - skip.locked() - skip.confirmed(),
                    row.remain(), null, null, false, ACTION_SKIPPED, null, false, false,
                    "草稿未发布，没有 quota:stock key，跳过比对");
        }
        if (!"PUBLISHED".equals(row.status())) {
            return writeFinalReport(resourceId);
        }

        Counts c = countRecords(resourceId);
        int dbIssued = c.locked() + c.confirmed();
        int dbRemain = row.totalQuota() - dbIssued;

        String raw = redis.opsForValue().get(RedisKeys.stock(resourceId));
        Integer redisRemain = parseRemain(raw);
        if (redisRemain == null) {
            // key 不存在或值不是整数：**不写 diff**。写 0 会造出 drift = −total_quota 的假差异，
            // 而这两件事的处置完全不同（一个要核账，一个要重新灌 key）。
            log.error("对账：库存 key 异常 resourceId={} raw={} totalQuota={} dbRemain={} —— "
                            + "抢占阶段读不到 key 会返回 -3「系统繁忙」，先重跑 POST /api/admin/resources/{}/publish 补 key，"
                            + "不要手工 SET（会绕开 onlyIfAbsent 的判断，覆盖掉正在被扣的值）",
                    resourceId, raw, row.totalQuota(), dbRemain, resourceId);
            return new ReconcileOutcome(resourceId, row.status(), row.totalQuota(),
                    c.records(), c.locked(), c.confirmed(), c.released(),
                    dbIssued, dbRemain, row.remain(), null, null,
                    true, ACTION_STOCK_KEY_MISSING, SEVERITY_WARN, false, false,
                    "库存 key 不存在或不是整数，未写 diff");
        }

        int drift = redisRemain - dbRemain;
        if (drift == 0) {
            return new ReconcileOutcome(resourceId, row.status(), row.totalQuota(),
                    c.records(), c.locked(), c.confirmed(), c.released(),
                    dbIssued, dbRemain, row.remain(), redisRemain, 0,
                    false, ACTION_NONE, null, false, false,
                    "三处相等，未写任何记录");
        }

        boolean consecutive = drift < 0 && consecutiveNegative(resourceId);
        // 正差异第一次就 WARN：Redis 比 DB 多，只可能是人工改过 Redis 或回补执行了两次，
        // 没有"等下一轮自己平"这种解释。负差异先 INFO，因为消息在途是常态。
        String severity = (drift > 0 || consecutive) ? SEVERITY_WARN : SEVERITY_INFO;
        insertDiff(row, dbRemain, redisRemain, drift, consecutive);
        boolean reportWritten = writeReport(row, c);

        String note = advice(drift, consecutive);
        if (SEVERITY_WARN.equals(severity)) {
            log.warn("对账：发现差异 resourceId={} drift={} redis={} db={} {}", resourceId, drift, redisRemain, dbRemain, note);
        } else {
            log.info("对账：发现差异（先按在途处理）resourceId={} drift={} redis={} db={}", resourceId, drift, redisRemain, dbRemain);
        }
        return new ReconcileOutcome(resourceId, row.status(), row.totalQuota(),
                c.records(), c.locked(), c.confirmed(), c.released(),
                dbIssued, dbRemain, row.remain(), redisRemain, drift,
                false, ACTION_DIFF, severity, consecutive, reportWritten, note);
    }

    /**
     * 活动关闭后的终版报表（R-7：每次发放都要留一份可归档的报表）。
     *
     * <p>只落 {@code quota_report}，<b>不做 Redis 比对</b>：活动关掉之后库存 key 被清是正常操作，
     * 拿它去比只会得到一条必然出现的假差异。要查历史账，看的是这张表里的 remain 和 balanced。
     */
    public ReconcileOutcome writeFinalReport(long resourceId) {
        ResourceRow row = loadResource(resourceId);
        Counts c = countRecords(resourceId);
        int dbIssued = c.locked() + c.confirmed();
        int dbRemain = row.totalQuota() - dbIssued;
        // 幂等：Job 那边已经用 NOT EXISTS 筛过一轮，这里再挡一次。
        // 手动触发可以对同一个已关闭活动连点，没这层就会一天堆出几十份一模一样的终版报表。
        boolean written = !hasReport(resourceId) && writeReport(row, c);
        return new ReconcileOutcome(resourceId, row.status(), row.totalQuota(),
                c.records(), c.locked(), c.confirmed(), c.released(),
                dbIssued, dbRemain, row.remain(), null, null,
                false, ACTION_REPORT_FINAL, null, false, written,
                written ? "已补一份终版报表" : "该活动已有报表，跳过");
    }

    /** 该活动是否已有报表。Job 用它决定要不要给已关闭的活动补终版。 */
    public boolean hasReport(long resourceId) {
        return reportMapper.selectLatest(resourceId) != null;
    }

    // ------------------------------------------------------------------ 内部

    /**
     * 负差异是否"已经是连续第二个周期"。
     *
     * <p>两条边都不能少：
     * <ul>
     *   <li>下界 {@code interval/2}：手动触发连点两次（间隔几秒）不该算一个周期。
     *       否则验收时连跑两次就会把"消息在途"误升级成警告。</li>
     *   <li>上界 {@code interval*2}：巡检停了一小时再跑，中间根本没观测过，
     *       不能因为"上一条也是负的"就认定连续——那期间账可能已经平了又坏。</li>
     * </ul>
     * 这个判断读的是 DB 里上一条 {@code SCAN} 记录，不用内存计数：多实例时内存各算各的，
     * 停机重启就丢，而表里的时间是共享的事实。
     */
    private boolean consecutiveNegative(long resourceId) {
        ReconcileDiff last = diffMapper.selectLatestScan(resourceId);
        if (last == null || last.getDrift() == null || last.getDrift() >= 0 || last.getDetectedAt() == null) {
            return false;
        }
        long gap = Duration.between(last.getDetectedAt(), AppTime.now()).toMillis();
        return gap >= intervalMs / 2 && gap <= intervalMs * 2;
    }

    private void insertDiff(ResourceRow row, int dbRemain, int redisRemain, int drift, boolean consecutive) {
        ReconcileDiff d = new ReconcileDiff();
        d.setResourceId(row.id());
        d.setRedisRemain(redisRemain);
        d.setDbRemain(dbRemain);
        d.setDrift(drift);
        // 判定时刻的配额上限：扩容后这个值会和新配额不同，是历史快照，不是错
        d.setTotalQuota(row.totalQuota());
        d.setSource("SCAN");
        d.setDetectedAt(precision3(AppTime.now()));
        d.setFixAction(advice(drift, consecutive));
        // student_id / record_id 留空：巡检是按活动聚合的，验不出"差的是哪几条"
        diffMapper.insert(d);
    }

    private boolean writeReport(ResourceRow row, Counts c) {
        // 平衡等式三条全成立才置 1：
        //   ① 三种状态的条数加起来等于记录总数（有第四种状态就说明枚举加了没同步这里）
        //   ② 仍有效的已发数不超过总名额（不超发）
        //   ③ remain 列和"由记录反推的余量"一致（列被绕过改过时这里会掉）
        int issued = c.locked() + c.confirmed();
        boolean balanced = (c.locked() + c.confirmed() + c.released() == c.records())
                && issued <= row.totalQuota()
                && row.remain() == row.totalQuota() - issued;

        QuotaReport p = new QuotaReport();
        p.setResourceId(row.id());
        p.setDeptId(row.deptId());
        p.setTotalQuota(row.totalQuota());
        // appliedCn 语义：**被拒的请求不落库**（没有对应的 quota_record），所以这里只能填记录数。
        // 表注释里"含重复提交"那半句做不到——重复提交被 Lua 的幂等键挡在 Redis 层，
        // 连记录都不会建。填一个凭空的申请量比填成功数更危险，宁可让这个列等于 granted。
        p.setAppliedCnt(c.records());
        p.setGrantedCnt(c.records());
        p.setConfirmedCnt(c.confirmed());
        p.setReleasedCnt(c.released());
        p.setLockedCnt(c.locked());
        p.setRemain(row.remain());
        p.setBalanced(balanced);
        p.setGeneratedAt(AppTime.now().truncatedTo(ChronoUnit.SECONDS)); // 列是 DATETIME(0)
        reportMapper.insert(p);
        return true;
    }

    private String advice(int drift, boolean consecutive) {
        if (drift > 0) {
            return "Redis 比 DB 多：核对 quota:stock 是否被手工改过、或释放回补执行了两次；不要直接把 DB 的 remain 写回 Redis";
        }
        if (consecutive) {
            return "连续两个周期为负：先 /drain 排除在途消息，仍为负说明有记录被删或落库失败，按 resource_id 查 quota_record 与消息轨迹";
        }
        return "负差异第一次：可能只是消息还在队列里。先 /drain 再看，连续两个周期仍为负才是真丢记录";
    }

    private ResourceRow loadResource(long resourceId) {
        List<ResourceRow> rows = jdbc.query(
                "SELECT id, dept_id, total_quota, remain, status FROM quota_resource WHERE id = ?",
                (rs, i) -> new ResourceRow(rs.getLong("id"), rs.getLong("dept_id"),
                        rs.getInt("total_quota"), rs.getInt("remain"), rs.getString("status")),
                resourceId);
        if (rows.isEmpty()) {
            throw new BizException(ErrorCode.RESOURCE_NOT_FOUND);
        }
        return rows.get(0);
    }

    private Counts countRecords(long resourceId) {
        Counts c = jdbc.queryForObject(
                """
                SELECT COUNT(*)                AS records,
                       SUM(status = 'LOCKED')    AS locked,
                       SUM(status = 'CONFIRMED') AS confirmed,
                       SUM(status = 'RELEASED')  AS released
                  FROM quota_record
                 WHERE resource_id = ?
                """,
                (rs, i) -> new Counts(rs.getInt("records"), rs.getInt("locked"),
                        rs.getInt("confirmed"), rs.getInt("released")),
                resourceId);
        return c == null ? new Counts(0, 0, 0, 0) : c;
    }

    private static Integer parseRemain(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Integer.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static LocalDateTime precision3(LocalDateTime t) {
        return t.truncatedTo(ChronoUnit.MILLIS);
    }

    private record ResourceRow(long id, long deptId, int totalQuota, int remain, String status) {
    }

    private record Counts(int records, int locked, int confirmed, int released) {
    }
}
