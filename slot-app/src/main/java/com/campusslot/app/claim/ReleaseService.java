package com.campusslot.app.claim;

import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.RedisKeys;
import com.campusslot.common.constant.StudentIdRule;
import com.campusslot.common.entity.QuotaRecord;
import com.campusslot.common.entity.QuotaResource;
import com.campusslot.common.entity.ReconcileDiff;
import com.campusslot.common.mapper.QuotaCollegeMapper;
import com.campusslot.common.mapper.QuotaRecordMapper;
import com.campusslot.common.mapper.QuotaResourceMapper;
import com.campusslot.common.mapper.QuotaRosterMapper;
import com.campusslot.common.mapper.ReconcileDiffMapper;
import com.campusslot.common.notify.NotificationService;
import com.campusslot.common.notify.NotifyEvent;
import com.campusslot.common.redis.RedisScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * 释放（{@code LOCKED → RELEASED}），两个入口共用一套事务 + 回补 + 对账：
 * <ul>
 *   <li>{@link #cancel} —— S-4，学生主动取消，按 {@code (resourceId, studentId)} 定位</li>
 *   <li>{@link #releaseExpired} —— SYS-1，扫表超时释放，按 {@code recordId} 定位</li>
 * </ul>
 *
 * <p><b>为什么不写成两个 Service</b>：两条路径的 DB 侧方向完全相同——状态迁移、两个余量 +1、
 * 上界判定、{@code release.lua} 回补、{@code reconcile_diff} 落账，唯一差别是
 * 「用什么条件定位记录」和「reason 写什么」。复制一份 130 行的事务+回补+对账，
 * 改一处就漂一处，而且改完得把 S-4 的八条验收重跑一遍。
 * 与 {@code compensate.lua} / {@code release.lua} 分成两个脚本的情形不同：那两个是
 * <b>方向相反</b>（一个 DEL 幂等键、一个必须保留），方向不同才该分开。
 *
 * <p><b>顺序是「DB 事务先、Redis 后」，不能反。</b>DB 是权威：
 * <ul>
 *   <li>先 DB：事务内完成状态迁移 + 两个余量 +1，提交后再 INCR Redis。
 *       若 Redis 这一步失败，结果是「Redis 少 1、DB 正确」——余量偏少（保守），
 *       D9 对账能检出并补齐。</li>
 *   <li>先 Redis：INCR 成功但 DB 事务回滚，Redis 多 1。这个 1 没有任何记录支撑，
 *       也没有第二次机会扣回——它会被别人抢走，就是<b>超发</b>。</li>
 * </ul>
 * 两种失败模式里必须选可逆的那个，这与 D3「回补错 = 超发红线」是同一条纪律。
 *
 * <p><b>{@code release.lua} 返回 -1/-2/-3/-4 时不回滚、不重试，只落对账。</b>
 * 三条理由：
 * <ol>
 *   <li>{@code -2} 本身就意味着 Redis 与 DB 已经漂了（Redis 侧已到 total 上界）。
 *       此时任何自动补偿都是在<b>用一个不可信的值去修正一个已提交的事实</b>。</li>
 *   <li>回滚要做 {@code DECR}，而 {@code DECR} 没有幂等语义：执行完未返回时崩溃、
 *       重试二次 DECR，会把别人刚扣掉的名额还回去 → 从「少发」变「超发」。</li>
 *   <li>{@code -2} 是确定性拒绝，重试还是 {@code -2}；用 {@code SET} 强写会覆盖并发扣减。</li>
 * </ol>
 * 所以唯一正确出口是「提交 + 落 {@code reconcile_diff} + 不重试」，把判断交给 D9。
 *
 * <p><b>不删 Redis 幂等键。</b>见 {@code release.lua} 头注释：删了它，学生再点会走完整扣减
 * 并投消息，消费端撞唯一索引 → DUPLICATE → ACK 且不回补 → 少发。
 */
@Service
public class ReleaseService {

    private static final Logger log = LoggerFactory.getLogger(ReleaseService.class);

    /**
     * 与 {@code quota_record.release_reason}（VARCHAR(32)）以及
     * {@code reconcile_diff.source}（VARCHAR(16)）共用同一组取值：
     * 一条记录的"怎么释放的"和"这笔对账是谁造成的"必须是同一个词，否则 D9 关联两张表要写字面量映射。
     */
    public static final String REASON_CANCEL = "CANCEL";
    public static final String REASON_TIMEOUT = "TIMEOUT";

    private static final String STATUS_LOCKED = "LOCKED";
    private static final String STATUS_CONFIRMED = "CONFIRMED";
    private static final String STATUS_RELEASED = "RELEASED";

    /** {@code release.lua} 的返回码，与脚本头注释一一对应。 */
    private static final int LUA_OK = 0;
    private static final int LUA_KEY_MISSING = -1;
    private static final int LUA_AT_TOTAL = -2;
    private static final int LUA_COLLEGE_KEY_MISSING = -3;
    private static final int LUA_COLLEGE_KEY_ALIASED = -4;

    private final QuotaRecordMapper recordMapper;
    private final QuotaResourceMapper resourceMapper;
    private final QuotaCollegeMapper collegeMapper;
    private final QuotaRosterMapper rosterMapper;
    private final ReconcileDiffMapper diffMapper;
    private final ResourceMetaCache metaCache;
    private final RedisScriptRunner scriptRunner;
    private final RedisScript<List> releaseScript;
    private final NotificationService notificationService;
    private final TransactionTemplate tx;

    public ReleaseService(QuotaRecordMapper recordMapper,
                          QuotaResourceMapper resourceMapper,
                          QuotaCollegeMapper collegeMapper,
                          QuotaRosterMapper rosterMapper,
                          ReconcileDiffMapper diffMapper,
                          ResourceMetaCache metaCache,
                          RedisScriptRunner scriptRunner,
                          @Qualifier("releaseScript") RedisScript<List> releaseScript,
                          NotificationService notificationService,
                          PlatformTransactionManager txManager) {
        this.recordMapper = recordMapper;
        this.resourceMapper = resourceMapper;
        this.collegeMapper = collegeMapper;
        this.rosterMapper = rosterMapper;
        this.diffMapper = diffMapper;
        this.metaCache = metaCache;
        this.scriptRunner = scriptRunner;
        this.releaseScript = releaseScript;
        this.notificationService = notificationService;
        this.tx = new TransactionTemplate(txManager);
    }

    /**
     * 取消一条 {@code (resourceId, studentId)} 的记录（S-4 入口）。
     *
     * <p>与确认一样是<b>同步写 DB</b>：单个人一次点击，量级比抢号低三个数量级，
     * 走队列再回查只会制造"点了没反应"。
     */
    public ReleaseOutcome cancel(long resourceId, String studentId) {
        if (!StudentIdRule.isValid(studentId)) {
            return ReleaseOutcome.fail(ErrorCode.PARAM_INVALID, StudentIdRule.rejectReason(studentId));
        }

        QuotaResource res = resourceMapper.selectById(resourceId);
        if (res == null) {
            return ReleaseOutcome.fail(ErrorCode.RESOURCE_NOT_FOUND, "资源不存在");
        }

        // 取消截止时间：用应用时钟，与 cancel_deadline 的写入口径一致（见 AppTime.now()）。
        LocalDateTime deadlineNow = AppTime.now();
        if (res.getCancelDeadline() != null && deadlineNow.isAfter(res.getCancelDeadline())) {
            return ReleaseOutcome.fail(ErrorCode.CANCEL_DEADLINE_PASSED,
                    "已过取消截止时间（" + res.getCancelDeadline() + "）");
        }

        return release(resourceId, studentId, null, REASON_CANCEL,
                resolveCollege(resourceId, studentId));
    }

    /**
     * 超时释放一条已经扫出来的记录（SYS-1 入口）。
     *
     * <p><b>不做截止时间检查、不做学号校验</b>：记录是 {@code selectExpiredLocked}
     * 按 {@code expire_at <= now} 查出来的，学生的取消截止时间在这里没有意义；
     * 学号来自数据库，不是外部输入。
     *
     * <p><b>按 {@code recordId} 定位，不按 {@code (resourceId, studentId)}</b>：
     * 扫描时已经拿到主键，再用复合条件查一次只是多一遍唯一索引；
     * 更重要的是 {@code releaseIfLocked} 的幂等语义就在这里——两个实例同时扫到同一条，
     * {@code WHERE status = 'LOCKED'} 只会让一边返回 1。
     */
    public ReleaseOutcome releaseExpired(QuotaRecord record) {
        long resourceId = record.getResourceId();
        String studentId = record.getStudentId();
        return release(resourceId, studentId, record.getId(), REASON_TIMEOUT,
                resolveCollege(resourceId, studentId));
    }

    /**
     * 两个入口的共同主体。
     *
     * @param recordId 非空表示按主键定位（SYS-1），空表示按 {@code (resourceId, studentId)} 定位（S-4）
     */
    private ReleaseOutcome release(long resourceId, String studentId, Long recordId,
                                   String reason, String collegeCode) {
        DbPart part;
        try {
            part = tx.execute(status -> doDb(resourceId, studentId, recordId, reason, collegeCode));
        } catch (RuntimeException e) {
            // 事务回滚：什么都没改，Redis 也还没动，可以直接报系统繁忙。
            // 这一处与"DB 先、Redis 后"的顺序强相关：正因为 Redis 在提交之后才动，
            // 这里才敢说"什么都没改"。SYS-1 拿到这个结果什么都不用做——
            // 记录仍是 LOCKED，下一轮扫描还会捡到它。
            log.error("释放事务失败 reason={} resourceId={} studentId={} recordId={}",
                    reason, resourceId, studentId, recordId, e);
            return ReleaseOutcome.fail(ErrorCode.SYSTEM_BUSY);
        }

        if (part == null) {
            log.error("释放事务未返回结果 reason={} resourceId={} studentId={}",
                    reason, resourceId, studentId);
            return ReleaseOutcome.fail(ErrorCode.SYSTEM_BUSY);
        }
        if (part.terminal() != null) {
            return part.terminal();
        }

        return afterCommit(resourceId, studentId, collegeCode, reason, part);
    }

    /**
     * 决定这次释放要不要回补学院余量。
     *
     * <p><b>判据与申领侧必须完全相同</b>：{@code AsyncClaimService} 只在
     * {@code meta.useCollegeQuota()} 为真时才查学院编码、才让 {@code claim.lua} 扣学院余量。
     * 释放如果不是同一个条件，就会出现「申领没扣、释放却加」——两边一起漂。
     *
     * <p>这个条件里有三项：活动要名单、{@code quota_college} 有行、全局开关
     * {@code campusslot.claim.college-quota-enabled} 打开。少判任何一项都会出问题：
     * <pre>
     *   只判"名单里有学院编码"：quota_roster 往往整表灌入（每行都带 college_code），
     *       而 quota_college 只有开了学院配额的活动才有行 →
     *       DB：UPDATE quota_college ... 影响 0 行 → 被误判成"学院余量已到顶" → 落假对账
     *       Redis：INCR quota:college:{rid}:{code} → key 从不存在 → 凭空造一个孤立 key
     *   少判全局开关：开关关着（默认就是 false）但表里有行时，申领侧从没碰过这个 key，
     *       释放侧却去加 —— 同一个坑，只是换成了"配置明明关着，却多出一堆 -3 对账"
     * </pre>
     *
     * <p><b>这里只挡住了"不该回补"，挡不住"该回补但 key 丢了"</b>——后者（Redis 重启后
     * 只重建了 stock、key 被误删）只有脚本能看见，由 {@code release.lua} 的 {@code EXISTS}
     * 检查返回 -3。两处缺一不可：一个判"要不要",一个判"能不能"。
     *
     * <p>代价：这条判据读的是 5 秒 TTL 的元信息缓存，活动刚加上学院行的 5 秒内
     * 释放不会回补学院余量。影响是"学院余量暂时少 1"，DB 与 Redis 仍然一致，D9 不需要介入。
     */
    private String resolveCollege(long resourceId, String studentId) {
        ResourceMeta meta = metaCache.get(resourceId);
        if (meta == null || !meta.useCollegeQuota()) {
            return null;
        }
        String collegeCode = rosterMapper.selectCollegeCode(resourceId, studentId);
        if (collegeCode == null || collegeCode.isBlank()) {
            log.debug("名单里没有学院编码，本次不回补学院余量 resourceId={} studentId={}",
                    resourceId, studentId);
            return null;
        }
        return collegeCode;
    }

    /**
     * 事务内：状态迁移 + 两个 DB 余量 +1 + 读配额上限。
     *
     * <p>返回 {@code terminal != null} 表示这次调用到此为止（幂等命中或状态冲突），
     * 不需要再动 Redis；否则返回给事务外继续用的一组快照。
     *
     * <p>注意这里<b>不</b>调 Redis —— 事务提交前动 Redis，一旦回滚就是"Redis 多 1、DB 没变"，
     * 那 1 个名额会被别人抢走，属于超发。
     */
    private DbPart doDb(long resourceId, String studentId, Long recordId,
                        String reason, String collegeCode) {
        LocalDateTime now = AppTime.now();

        int rows = recordId == null
                ? recordMapper.releaseByStudent(resourceId, studentId, reason, now)
                : recordMapper.releaseIfLocked(recordId, reason, now);

        if (rows == 0) {
            if (recordId != null) {
                // SYS-1 扫到的记录已被处理。能赢的只可能是学生取消（取消侧刻意不判
                // expire_at）或另一个实例的扫描——确认要求 expire_at > now，与扫描条件互斥。
                // 这不是错误，是正常的并发收敛，所以不落对账、不打 warn。
                log.debug("超时释放跳过（记录已不是 LOCKED）resourceId={} studentId={} recordId={}",
                        resourceId, studentId, recordId);
                return DbPart.terminal(ReleaseOutcome.alreadyReleased(null));
            }

            // 四种可能必须分开答，HTTP 语义完全不同（200 / 409 / 404）
            QuotaRecord record = recordMapper.selectByStudent(resourceId, studentId);
            if (record == null) {
                return DbPart.terminal(ReleaseOutcome.fail(ErrorCode.RECORD_NOT_FOUND,
                        "没有找到你的申领记录"));
            }
            String status = record.getStatus();
            if (STATUS_RELEASED.equals(status)) {
                // 幂等：重复取消不是错误，返回 200（前端重复点击不该弹红框）
                return DbPart.terminal(ReleaseOutcome.replay(record.getReleaseReason()));
            }
            if (STATUS_CONFIRMED.equals(status)) {
                // 目标状态与当前状态互斥：已确认的名额不能取消（名额已经归属别人）
                return DbPart.terminal(ReleaseOutcome.fail(ErrorCode.RECORD_STATUS_ILLEGAL,
                        "名额已确认，无法取消"));
            }
            if (STATUS_LOCKED.equals(status)) {
                // UPDATE 条件与回查结果矛盾：只可能是并发窗口或 SQL 被改坏。
                // 不能猜，记 warn 后按状态冲突拒绝——与 ConfirmService 同一处置。
                log.warn("取消返回 0 但记录仍是 LOCKED —— 条件与回查不一致 resourceId={} studentId={}",
                        resourceId, studentId);
                return DbPart.terminal(ReleaseOutcome.fail(ErrorCode.RECORD_STATUS_ILLEGAL));
            }
            log.error("quota_record 出现未知状态 status={} resourceId={} studentId={}",
                    status, resourceId, studentId);
            return DbPart.terminal(ReleaseOutcome.fail(ErrorCode.SYSTEM_BUSY));
        }

        // 状态迁移成功。取消路径回查一次拿 recordId（对账记录要能追溯到具体那条），
        // 这一步在事务内、走唯一索引，代价远低于"对账时查不到是哪条记录"的麻烦。
        // 超时路径的主键本来就有，不再查——这是两个入口唯一的取 id 方式差异。
        Long recId = recordId;
        if (recId == null) {
            QuotaRecord record = recordMapper.selectByStudent(resourceId, studentId);
            if (record == null) {
                // 理论上不会发生（rows==1 才走到这里）；留一条防御，避免 NPE 把成功变成失败
                log.warn("释放成功后回查不到记录 resourceId={} studentId={}", resourceId, studentId);
            } else {
                recId = record.getId();
            }
        }

        // DB 权威余量 +1。返回 0 表示已经顶到 total_quota。正常情况不该发生：
        // 既然有一条 LOCKED 记录占着名额，remain 就不可能已经是 total。
        // 真发生了就是两边已经漂了，留给事务外落对账（那里能拿到真实的 Redis 值）。
        boolean dbAtTotal = resourceMapper.increaseRemain(resourceId, now) == 0;

        boolean collegeAtTotal = false;
        if (collegeCode != null) {
            collegeAtTotal = collegeMapper.increaseRemain(resourceId, collegeCode, now) == 0;
        }

        // 事务内当前读（FOR UPDATE）：total_quota 若是快照读，RR 下可能拿到扩容前的旧值，
        // 于是合法回补被判成 -2，落一条假对账。同事务内读保证「DB 已 +1」与「上界判定」
        // 基于同一个提交点。
        Map<String, Object> row = resourceMapper.selectRemainAndTotalForUpdate(resourceId);
        int dbRemain = intOf(row, "remain");
        int totalQuota = intOf(row, "total_quota");

        // 通知入账（SYS-2）：同一个事务，与状态迁移一起成败。
        // <b>前缀由 reason 派生，不由入口派生</b>：S-4 与 SYS-1 共用这个方法，
        // 若在入口写死前缀，将来加第三个 reason 会静默写错前缀——而 bizId 是唯一键，
        // 写错不报错，只会多出一条永远不会被合并的重复通知。
        notificationService.enqueue(
                REASON_CANCEL.equals(reason) ? NotifyEvent.CANCEL : NotifyEvent.RELEASE,
                resourceId, studentId);

        return new DbPart(null, recId, dbRemain, totalQuota, dbAtTotal, collegeAtTotal);
    }

    /** 事务已提交：INCR Redis 镜像余量，并把所有拒绝原因落成对账记录。 */
    private ReleaseOutcome afterCommit(long resourceId, String studentId,
                                       String collegeCode, String reason, DbPart part) {
        List<String> keys = List.of(
                RedisKeys.stock(resourceId),
                collegeCode == null ? RedisKeys.stock(resourceId)
                        : RedisKeys.collegeStock(resourceId, collegeCode));

        List<String> raw;
        try {
            raw = scriptRunner.eval(releaseScript, keys,
                    String.valueOf(part.totalQuota()), collegeCode == null ? "0" : "1");
        } catch (RuntimeException e) {
            // Redis 不可用：DB 已提交，状态迁移是既成事实，不能回滚（也回滚不了）。
            // 结果是"Redis 少 1"，方向保守，D9 对账会检出并补齐。
            log.error("释放后 Redis 回补失败，DB 已提交 reason={} resourceId={} studentId={} dbRemain={}",
                    reason, resourceId, studentId, part.dbRemain(), e);
            // 这里的 redisRemain 传 0 只是占位：Redis 都不可达了，真实值根本未知。
            // 后果是 drift 被算成 -dbRemain（例如 -99），看起来像"整个 key 没了"。
            // 语义确实被污染了，但修复动作相同（对齐 DB），且 fix_action=REDIS_UNAVAILABLE
            // 本身就让 D9 能区分这两种完全不同的情况——所以不改 drift，改注释。
            saveDiff(resourceId, studentId, part, reason, 0, "REDIS_UNAVAILABLE");
            return ReleaseOutcome.okPending(reason, STATUS_RELEASED, part.dbRemain(),
                    "已取消，余量同步稍后由对账补齐");
        }

        int code = intOf(raw, 0);
        int redisRemain = raw != null && raw.size() > 1 ? intOf(raw, 1) : 0;

        if (code == LUA_OK) {
            if (part.dbAtTotal() || part.collegeAtTotal()) {
                // Redis 侧加成功了，DB 侧却已经在上界——这本身就说明两边漂了。
                // 不阻断用户（他的取消是真的），但要留证据，否则 D9 永远看不到这一笔。
                String action = part.dbAtTotal() ? "DB_AT_TOTAL" : "COLLEGE_AT_TOTAL";
                log.warn("释放成功但 DB 侧已在配额上界 reason={} resourceId={} studentId={} action={} "
                                + "dbRemain={} totalQuota={} redisRemain={}",
                        reason, resourceId, studentId, action, part.dbRemain(),
                        part.totalQuota(), redisRemain);
                saveDiff(resourceId, studentId, part, reason, redisRemain, action);
            }
            log.debug("释放成功 reason={} resourceId={} studentId={} dbRemain={} redisRemain={}",
                    reason, resourceId, studentId, part.dbRemain(), redisRemain);
            return ReleaseOutcome.ok(reason, STATUS_RELEASED, redisRemain);
        }

        // 非 0：Redis 侧拒绝了这次回补，DB 已经提交了 +1，必须留证据给 D9。
        String action = switch (code) {
            case LUA_KEY_MISSING -> "STOCK_KEY_MISSING";
            case LUA_AT_TOTAL -> "REDIS_AT_TOTAL";
            case LUA_COLLEGE_KEY_MISSING -> "COLLEGE_KEY_MISSING";
            case LUA_COLLEGE_KEY_ALIASED -> "COLLEGE_KEY_ALIASED";
            // 脚本新增了返回码但这里没跟上：不能猜它属于哪一类，落最保守的名字。
            default -> "UNKNOWN_CODE_" + code;
        };
        log.warn("释放回补被 Redis 拒绝 code={} reason={} resourceId={} studentId={} "
                        + "dbRemain={} redisRemain={} college={}",
                code, reason, resourceId, studentId, part.dbRemain(), redisRemain, collegeCode);
        saveDiff(resourceId, studentId, part, reason, redisRemain, action);

        // 仍然返回成功：DB 侧的状态迁移是真的，用户的名额确实释放了。
        // 报失败会让他重试，而重试会走幂等分支，什么也不会改变。
        return ReleaseOutcome.okPending(reason, STATUS_RELEASED, redisRemain,
                "已取消，余量同步待对账修复");
    }

    /**
     * 落一条对账差异。{@code fixed_at} 留空 = 未修，D9 修完回填。
     *
     * <p>用 {@code insert} 而不是 upsert：同一个资源短时间内可能产生多条事件，
     * 合并会丢掉"发生过几次"这个信息，而 D9 恰恰要靠它判断是偶发还是系统性问题。
     *
     * <p>{@code source} 取本次释放的 reason（{@code CANCEL} / {@code TIMEOUT}），
     * 与巡检产生的 {@code SCAN} 区分——三类记录共用一张表，但"这是某一笔取消造成的"、
     * "这是超时扫描造成的"和"巡检发现累计差 N"必须能分开读。
     */
    private void saveDiff(long resourceId, String studentId, DbPart part,
                          String reason, int redisRemain, String action) {
        try {
            ReconcileDiff diff = new ReconcileDiff();
            diff.setResourceId(resourceId);
            diff.setRedisRemain(redisRemain);
            diff.setDbRemain(part.dbRemain());
            diff.setDrift(redisRemain - part.dbRemain());
            diff.setFixAction(action);
            diff.setDetectedAt(AppTime.now());
            diff.setSource(reason);
            diff.setStudentId(studentId);
            diff.setRecordId(part.recordId());
            diff.setTotalQuota(part.totalQuota());
            diffMapper.insert(diff);
        } catch (RuntimeException e) {
            // 对账写不进去就只能打日志：不能因为"记账失败"把一个已经成立的释放变成失败
            log.error("对账记录写入失败 reason={} resourceId={} studentId={} action={} "
                            + "redisRemain={} dbRemain={}",
                    reason, resourceId, studentId, action, redisRemain, part.dbRemain(), e);
        }
    }

    private static int intOf(Map<String, Object> row, String key) {
        if (row == null) {
            return 0;
        }
        Object v = row.get(key);
        return v instanceof Number n ? n.intValue() : 0;
    }

    private static int intOf(List<String> raw, int index) {
        if (raw == null || raw.size() <= index) {
            return 0;
        }
        try {
            return Integer.parseInt(raw.get(index));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * 事务结果。{@code terminal != null} 表示不用继续（幂等命中或状态冲突），
     * 否则带着提交后的快照去动 Redis。
     */
    private record DbPart(ReleaseOutcome terminal, Long recordId, int dbRemain, int totalQuota,
                          boolean dbAtTotal, boolean collegeAtTotal) {

        static DbPart terminal(ReleaseOutcome outcome) {
            return new DbPart(outcome, null, 0, 0, false, false);
        }
    }
}
