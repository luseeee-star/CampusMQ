package com.campusslot.app.resource;

import com.campusslot.app.claim.ResourceMetaCache;
import com.campusslot.app.resource.dto.CollegeQuotaItem;
import com.campusslot.app.resource.dto.CreateResourceRequest;
import com.campusslot.app.resource.dto.CreateResourceResult;
import com.campusslot.app.resource.dto.PublishResult;
import com.campusslot.app.resource.dto.RosterItem;
import com.campusslot.common.api.BizException;
import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.StudentIdRule;
import com.campusslot.common.entity.Dept;
import com.campusslot.common.entity.QuotaCollege;
import com.campusslot.common.entity.QuotaResource;
import com.campusslot.common.enums.ResourceStatus;
import com.campusslot.common.mapper.DeptMapper;
import com.campusslot.common.mapper.QuotaCollegeMapper;
import com.campusslot.common.mapper.QuotaResourceMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 管理端：建活动（G-1 + G-2 并入）与发布（DRAFT → PUBLISHED，灌 Redis）。
 *
 * <p><b>为什么"建活动"和"发布"是两个接口，而不是建的时候直接上线</b>：
 * 名单是分批写的，几千上万行；如果建完就自动发布，中间任何一次失败都会留下一个
 * "已上线但名单不全"的活动，表现是名单里的人抢不到、而且不报错。
 * 拆开之后中间有一个 DRAFT 状态兜着——草稿抢不了，失败就停在草稿，重来一次即可。
 *
 * <p><b>事务边界是刻意的</b>：
 * <ul>
 *   <li>{@code quota_resource} + {@code quota_college} 在<b>一个</b>事务里（几行，很快）；</li>
 *   <li>名单<b>不在</b>那个事务里，每 1000 行独立提交。塞进同一事务会持锁很久、
 *       撤销日志也大，而且它失败时最坏的结果只是"草稿里的名单不全"，没人能抢。</li>
 * </ul>
 */
@Service
public class ResourceAdminService {

    private static final Logger log = LoggerFactory.getLogger(ResourceAdminService.class);

    /** 名单批量写入的批大小。和 seeder 的 pipeline 批大小无关，后者管 Redis、这个管 JDBC。 */
    private static final int ROSTER_BATCH = 1000;

    private static final int DEFAULT_CONFIRM_TIMEOUT_SEC = 1800;

    private static final String INSERT_ROSTER_SQL = """
            INSERT INTO quota_roster (resource_id, student_id, college_code, extra)
            VALUES (?, ?, ?, 'api')
            ON DUPLICATE KEY UPDATE college_code = VALUES(college_code)
            """;

    private final QuotaResourceMapper resourceMapper;
    private final QuotaCollegeMapper collegeMapper;
    private final DeptMapper deptMapper;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final QuotaRedisSeeder seeder;
    private final ResourceMetaCache metaCache;

    public ResourceAdminService(QuotaResourceMapper resourceMapper,
                                QuotaCollegeMapper collegeMapper,
                                DeptMapper deptMapper,
                                JdbcTemplate jdbc,
                                PlatformTransactionManager txManager,
                                QuotaRedisSeeder seeder,
                                ResourceMetaCache metaCache) {
        this.resourceMapper = resourceMapper;
        this.collegeMapper = collegeMapper;
        this.deptMapper = deptMapper;
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(txManager);
        this.seeder = seeder;
        this.metaCache = metaCache;
    }

    // ==================================================================
    // 建活动
    // ==================================================================

    /**
     * 建一个草稿活动，顺带写入学院配额与资格名单。
     *
     * <p>校验的顺序是有意的：<b>先校验、再落库</b>。所有"配置不完整"的情况都在写第一行之前拒绝，
     * 因为名单可能有几万行，落完一半再报错的代价不只是慢——它会留下一个看起来正常、实际缺人的草稿。
     */
    public CreateResourceResult create(CreateResourceRequest req) {
        boolean rosterRequired = Boolean.TRUE.equals(req.rosterRequired());

        Dept dept = deptMapper.selectById(req.deptId());
        if (dept == null || dept.getStatus() == null || dept.getStatus() != 1) {
            // quota_resource.dept_id 只有普通索引、没有外键，传 99 也能插进去变成脏数据，
            // 所以存在性必须在应用层判，而且顺带判 status=1（停用的部门不该再建新活动）。
            throw new BizException(ErrorCode.PARAM_INVALID,
                    "部门不存在或已停用 deptId=" + req.deptId());
        }

        validateTimeWindow(req);

        Map<String, Integer> colleges = normalizeColleges(req.colleges(), req.totalQuota(), rosterRequired);
        Map<String, String> roster = normalizeRoster(req.roster(), rosterRequired);
        int rosterDuplicates = (req.roster() == null ? 0 : req.roster().size()) - roster.size();

        int confirmTimeoutSec = req.confirmTimeoutSec() == null
                ? DEFAULT_CONFIRM_TIMEOUT_SEC : req.confirmTimeoutSec();

        Long insertedId = tx.execute(status -> {
            QuotaResource res = new QuotaResource();
            res.setDeptId(req.deptId());
            res.setTitle(req.title().trim());
            res.setDescription(req.description() == null ? null : req.description().trim());
            res.setTotalQuota(req.totalQuota());
            // 新建的草稿 remain = total；发布时灌 Redis 取的是 remain，不是 total
            res.setRemain(req.totalQuota());
            res.setStatus(ResourceStatus.DRAFT.name());
            res.setOpenAt(req.openAt());
            res.setCloseAt(req.closeAt());
            res.setCancelDeadline(req.cancelDeadline());
            res.setConfirmTimeoutSec(confirmTimeoutSec);
            res.setRosterRequired(rosterRequired);
            resourceMapper.insert(res);

            for (Map.Entry<String, Integer> e : colleges.entrySet()) {
                QuotaCollege c = new QuotaCollege();
                c.setResourceId(res.getId());
                c.setCollegeCode(e.getKey());
                c.setQuota(e.getValue());
                // 建行时 remain = quota。不让外部传 remain：那能造出"剩 5 但配额 20"的活动，
                // 发布后余量和配额立刻对不上，而两边都"合法"，查起来极慢。
                c.setRemain(e.getValue());
                collegeMapper.insert(c);
            }
            return res.getId();
        });
        if (insertedId == null) {
            // 事务回调正常返回时不可能为 null；真出现说明事务被外部标了 rollback-only，
            // 这时候继续写名单会往一个不存在的 resourceId 上灌数据
            throw new BizException(ErrorCode.SYSTEM_BUSY, "建活动事务未返回主键，已回滚");
        }
        long resourceId = insertedId;

        int rosterRows = insertRoster(resourceId, roster);

        List<String> warnings = new ArrayList<>();
        if (rosterDuplicates > 0) {
            warnings.add("名单里有 " + rosterDuplicates + " 行重复学号，已按最后一次出现的学院编码去重");
        }
        if (rosterRequired && rosterRows < roster.size()) {
            warnings.add("名单预期 " + roster.size() + " 行，实际写入 " + rosterRows + " 行，发布前请核对");
        }
        log.info("建活动完成 resourceId={} deptId={} total={} colleges={} roster={} rosterRequired={}",
                resourceId, req.deptId(), req.totalQuota(), colleges.size(), rosterRows, rosterRequired);

        return new CreateResourceResult(
                resourceId,
                ResourceStatus.DRAFT.name(),
                req.deptId(),
                req.totalQuota(),
                req.totalQuota(),
                req.openAt(),
                req.closeAt(),
                req.cancelDeadline(),
                confirmTimeoutSec,
                rosterRequired,
                colleges.size(),
                rosterRows,
                rosterDuplicates,
                warnings);
    }

    private void validateTimeWindow(CreateResourceRequest req) {
        if (!req.closeAt().isAfter(req.openAt())) {
            throw new BizException(ErrorCode.PARAM_INVALID,
                    "closeAt 必须晚于 openAt（库里有 CHECK close_at > open_at，不先挡住就是 500）");
        }
        LocalDateTime cancel = req.cancelDeadline();
        if (cancel != null && (cancel.isBefore(req.openAt()) || cancel.isAfter(req.closeAt()))) {
            // 早于开放时刻没有意义（那时还不能申领），晚于关闭时刻等于没有截止
            throw new BizException(ErrorCode.PARAM_INVALID,
                    "cancelDeadline 必须落在 [openAt, closeAt] 内；不传表示关闭前都可取消");
        }
    }

    /**
     * 学院配额归一化：去空白、查重、校验合计不超过总名额。
     *
     * <p><b>最后那条"没有名单就不许配学院配额"不是洁癖</b>：{@code needCollegeQuota()}
     * 的第一个条件就是"活动要名单"，而学院编码只能从名单里取。配了学院行但没有名单，
     * 结果是一批永远不会被扣的 {@code quota:college:*} key——报表上看着有配额，
     * 实际拦不住任何人。这种"配了等于没配"的状态没有任何报错，必须在这里挡掉。
     */
    private Map<String, Integer> normalizeColleges(List<CollegeQuotaItem> items, int totalQuota,
                                                   boolean rosterRequired) {
        Map<String, Integer> out = new LinkedHashMap<>();
        if (items == null || items.isEmpty()) {
            return out;
        }
        if (!rosterRequired) {
            throw new BizException(ErrorCode.PARAM_INVALID,
                    "配了学院配额但 rosterRequired 不为 true：学院编码来自资格名单，没有名单这一层永远不会生效");
        }
        long sum = 0;
        for (CollegeQuotaItem item : items) {
            String code = item.collegeCode().trim();
            if (out.putIfAbsent(code, item.quota()) != null) {
                throw new BizException(ErrorCode.PARAM_INVALID,
                        "学院编码重复：" + code + "（uk_college_resource_code 会挡住，先在这里说清楚）");
            }
            sum += item.quota();
        }
        if (sum > totalQuota) {
            // 不是"技术上跑不通"（总余量会先挡住），而是运营口径错了：
            // 各学院加起来比总名额还多，页面会承诺一个发不出来的数
            throw new BizException(ErrorCode.PARAM_INVALID,
                    "学院配额合计 " + sum + " 超过总名额 " + totalQuota);
        }
        return out;
    }

    /**
     * 名单归一化：去空白、校验学号、<b>强制学院编码</b>、去重。
     *
     * <p>学院编码必填的理由见 {@code ResourceMeta#needRosterCheck()}：
     * 名单成员判定靠"取得到学院编码"，而灌入时会静默跳过空值——整份名单灌完 0 个 key，
     * 发布照样成功，抢的时候全场 1005。校验放在这里而不是注解上，是为了能报出"第几行、哪个学号"。
     */
    private Map<String, String> normalizeRoster(List<RosterItem> items, boolean rosterRequired) {
        Map<String, String> out = new LinkedHashMap<>();
        if (items == null || items.isEmpty()) {
            if (rosterRequired) {
                // 没有单独的"导入名单"接口，建的时候不写就没有第二次机会；
                // 而 rosterRequired=1 + 空名单 = 一场谁都抢不到的活动
                throw new BizException(ErrorCode.PARAM_INVALID,
                        "rosterRequired=1 但名单为空：发布后会全场 1005，且不会有任何报错");
            }
            return out;
        }
        if (!rosterRequired) {
            throw new BizException(ErrorCode.PARAM_INVALID,
                    "rosterRequired 不为 true 时不要传 roster：名单不会被检查，等于没配");
        }
        int row = 0;
        for (RosterItem item : items) {
            row++;
            String studentId = item.studentId() == null ? null : item.studentId().trim();
            if (!StudentIdRule.isValid(studentId)) {
                throw new BizException(ErrorCode.PARAM_INVALID,
                        "名单第 " + row + " 行学号不合法：" + StudentIdRule.rejectReason(studentId));
            }
            String college = item.collegeCode() == null ? null : item.collegeCode().trim();
            if (college == null || college.isEmpty()) {
                throw new BizException(ErrorCode.PARAM_INVALID,
                        "名单第 " + row + " 行（学号 " + studentId + "）缺少学院编码："
                                + "灌名单时会静默跳过这一行，该学生会被判成不在名单内");
            }
            // 同一份名单里重复的学号按"后写覆盖"处理（ON DUPLICATE KEY UPDATE 的语义），
            // 这里先去重，是为了让返回的条数和实际写入条数对得上
            out.put(studentId, college);
        }
        return out;
    }

    /**
     * 分批写名单，每批独立提交。
     *
     * <p>用 {@code ON DUPLICATE KEY UPDATE} 而不是纯 INSERT：唯一键
     * {@code uk_roster_resource_student} 已经存在，重复导入应该幂等，而不是炸掉整批。
     * 注意 MySQL 对这条语句的"影响行数"口径是 1=插入、2=更新、0=值没变，
     * 所以<b>不能用受影响行数当写入条数</b>，这里直接按提交的行数返回。
     */
    private int insertRoster(long resourceId, Map<String, String> roster) {
        if (roster.isEmpty()) {
            return 0;
        }
        List<Map.Entry<String, String>> entries = new ArrayList<>(roster.entrySet());
        int written = 0;
        for (int from = 0; from < entries.size(); from += ROSTER_BATCH) {
            int to = Math.min(from + ROSTER_BATCH, entries.size());
            List<Object[]> args = new ArrayList<>(to - from);
            for (int i = from; i < to; i++) {
                Map.Entry<String, String> e = entries.get(i);
                args.add(new Object[]{resourceId, e.getKey(), e.getValue()});
            }
            jdbc.batchUpdate(INSERT_ROSTER_SQL, args);
            written += args.size();
        }
        return written;
    }

    // ==================================================================
    // 发布
    // ==================================================================

    /**
     * 发布：把三类 key 灌进 Redis，最后迁状态。
     *
     * <p><b>顺序不能反</b>（先灌 Redis、后迁移状态）：中途失败时活动还停在 DRAFT，
     * 重跑能走覆盖写那条分支；反过来先迁移状态，失败后就是 PUBLISHED + key 缺失，
     * 而缺的 key 只能靠"只补缺"慢慢补——补的前提是有人知道缺了什么。
     *
     * <p><b>两条分支的判据是"当前状态"</b>，不是"这次调用是不是第一次"：
     * <ul>
     *   <li>DRAFT → 覆盖写，值取数据库的 {@code remain}。没上线、无并发，覆盖正是为了校正上一次的残留。</li>
     *   <li>PUBLISHED → 只补缺（SETNX）。有人在抢，覆盖等于把已经发出去的又加回去。</li>
     * </ul>
     * 不要写"状态迁移影响 0 行就直接返回成功、不碰 Redis"：如果上次灌 Redis 失败发生在
     * 状态提交之后，status 已经是 PUBLISHED，再调必然 0 行，缺的 key 就永远没人补。
     *
     * <p><b>只补缺那条分支有一个已知的边界，必须知道</b>：库存 key 丢失时，
     * 回填值取的是数据库的 {@code remain}。如果此刻消费端还没追平（队列里有消息没落库），
     * {@code remain} 比真实余量<b>大</b>，回填就把在途的再放了一遍——2026-11-05 验收时实测到
     * {@code pending = -2}、{@code drift = +2}。不填更糟（全场 -3），所以还是填，
     * 但会打一条 P0 error 并在返回里带提示：发布后先 {@code /api/claim/dev/drain}，
     * 再看 {@code stat} 的 {@code drift} 是否为 0。这条只能靠对账收敛，不能靠接口猜。
     */
    public PublishResult publish(long resourceId) {
        QuotaResource res = resourceMapper.selectById(resourceId);
        if (res == null) {
            throw new BizException(ErrorCode.RESOURCE_NOT_FOUND);
        }
        String before = res.getStatus();
        boolean draft;
        if (ResourceStatus.DRAFT.name().equals(before)) {
            draft = true;
        } else if (ResourceStatus.PUBLISHED.name().equals(before)) {
            draft = false;
        } else {
            // CLOSED / OFFLINE 不该被"重新发布"：那等于绕过下架，学生还能继续抢
            throw new BizException(ErrorCode.RESOURCE_STATUS_ILLEGAL,
                    "只有 DRAFT / PUBLISHED 可以发布，当前状态 " + before);
        }

        int rosterRows = seeder.countRoster(resourceId);
        if (Boolean.TRUE.equals(res.getRosterRequired()) && rosterRows == 0) {
            // 专挡"名单写到一半就被发布"：那种情况的表现是全场 1005，而且不报错
            throw new BizException(ErrorCode.PARAM_INVALID,
                    "rosterRequired=1 但名单 0 行，拒绝发布：发布后全场 1005 且不会有任何报错");
        }

        List<QuotaCollege> colleges = collegeMapper.listByResource(resourceId);
        boolean onlyIfAbsent = !draft;
        int remain = res.getRemain() == null ? 0 : res.getRemain();

        // 三类 key 都灌：余量、各学院余量、名单。
        // 学院 key 即使当前 needCollegeQuota() 为 false 也要灌——那是"开不开"的运行时决策，
        // 现在不灌、以后再打开开关，就会出现"扣一个从未存在的 key"→ 全场 -3。
        int stockWritten = seeder.seedStock(resourceId, remain, onlyIfAbsent);
        int collegeWritten = seeder.seedColleges(resourceId, onlyIfAbsent);
        int rosterWritten = seeder.seedRoster(resourceId, onlyIfAbsent);

        boolean migrated = false;
        if (draft) {
            migrated = jdbc.update("""
                    UPDATE quota_resource
                       SET status = 'PUBLISHED', version = version + 1, updated_at = ?
                     WHERE id = ? AND status = 'DRAFT'
                    """, AppTime.now(), resourceId) > 0;
        }

        // 缓存里存着状态和开放时刻，最长 5 秒才自然过期；不清的话"发布后立刻抢"
        // 可能被判成"活动还没开放"，演示时看起来像发布没生效
        metaCache.evict(resourceId);

        List<String> notes = new ArrayList<>();
        if (onlyIfAbsent && stockWritten == 1) {
            // 实测（A1 验收时踩到的）：删掉 stock key 再重发，回填值取的是 DB 的 remain；
            // 若此刻还有消息在队列里没落库，DB remain 比真实余量<b>大</b>，回填等于把在途的又放一遍。
            // 不填更糟（全场 -3），所以填，但必须留 ERROR 和提示——这条只能靠对账收敛，不能靠猜。
            log.error("P0 库存 key 原本缺失，已按数据库 remain={} 回填 resourceId={}；"
                            + "若消费端未追平，在途消息对应的名额会被重复放出", remain, resourceId);
            notes.add("库存 key 原本缺失，已按数据库 remain=" + remain + " 回填。"
                    + "若发布时还有消息未落库，这个值会偏大（多放出 pending 个名额）："
                    + "发布后先 /api/claim/dev/drain 等账平，再看 stat 的 drift 是否为 0");
        }
        if (onlyIfAbsent) {
            notes.add("当前已是 PUBLISHED，本次只补缺（SETNX）；没写的 key 说明本来就存在");
        }
        if (draft && !migrated) {
            notes.add("状态迁移影响 0 行：并发下另一次发布已经完成了迁移，Redis 已按同样口径灌好");
        }
        if (!colleges.isEmpty() && !Boolean.TRUE.equals(res.getRosterRequired())) {
            notes.add("该活动没开名单，学院配额行不会生效（只入库、不拦截），学院 key 仍已灌好");
        }
        log.info("发布完成 resourceId={} {}→{}(migrated={}) stock={}(written={}) college={}/{} roster={}/{} onlyIfAbsent={}",
                resourceId, before, draft ? ResourceStatus.PUBLISHED.name() : before, migrated,
                remain, stockWritten, collegeWritten, colleges.size(), rosterWritten, rosterRows, onlyIfAbsent);

        return new PublishResult(
                resourceId,
                before,
                draft && migrated ? ResourceStatus.PUBLISHED.name() : before,
                migrated,
                onlyIfAbsent,
                remain,
                colleges.size(),
                collegeWritten,
                rosterRows,
                rosterWritten,
                notes);
    }
}
