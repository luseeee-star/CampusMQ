package com.campusslot.app.claim;

import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.api.R;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.RedisKeys;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 实验用的复位 / 判据 / 等追平接口，只在 {@code campusslot.dev-endpoints=true} 时装配。
 *
 * <p><b>为什么必须有 reset</b>：每轮压测前要把 <b>Redis 和 DB 同时</b>拉回同一条起跑线。
 * 只重置其中一边，下一轮从一开始就是漂移状态，超卖数会混进上一轮的残留，整组数据作废。
 * D3 起多了一个「还没落库」的维度：如果 reset 只清 DB 不清 MQ，上一轮的滞留消息
 * 会在新一轮里落库，表现为「records 比成功数还多」，同样作废。
 *
 * <p><b>为什么必须有 drain</b>：异步落库之后，压测器跑完那一刻
 * {@code quota_record} 一定比"成功数"少——消息还在队列里。直接读 stat 会得出
 * 「丢了 40 条」的结论。{@link #drain} 等到账平了再读，顺便把「追平耗时」
 * 这个数字量出来（D10 要画的就是这条曲线）。
 *
 * <p><b>为什么判据不能沿用 D1</b>：D1 的 {@code bare_activity.remain} 允许为负，
 * 判据是 {@code remain < 0}。D2 起用主表，有 {@code CHECK (remain >= 0)}，
 * 超卖表现为 <b>记录数超过总名额</b>。D3 又多一层：还要看 {@code pending}。
 * 用错判据会把"没有超卖"读成"实验失败"，也会把"消息还没消费完"读成"丢消息"。
 */
@RestController
@RequestMapping("/api/claim/dev")
@ConditionalOnProperty(name = "campusslot.dev-endpoints", havingValue = "true")
public class ClaimDevController {

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final ResourceMetaCache metaCache;
    private final com.campusslot.common.redis.RedisScriptRunner scriptRunner;
    private final com.campusslot.app.notify.MockNotifySender mockNotifySender;
    private final com.campusslot.common.mapper.NotificationMapper notificationMapper;

    public ClaimDevController(JdbcTemplate jdbc, StringRedisTemplate redis, ResourceMetaCache metaCache,
                              com.campusslot.common.redis.RedisScriptRunner scriptRunner,
                              com.campusslot.app.notify.MockNotifySender mockNotifySender,
                              com.campusslot.common.mapper.NotificationMapper notificationMapper) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.metaCache = metaCache;
        this.scriptRunner = scriptRunner;
        this.mockNotifySender = mockNotifySender;
        this.notificationMapper = notificationMapper;
    }

    /**
     * 运行时切换脚本执行路径，供 A/B 判决使用。
     *
     * <p><b>为什么必须是运行时切换，而不是"改配置再重启"</b>：重启换来的冷启动差异
     * （客户端建连、JIT 档位）实测有 0.62 的系数，和要测的效应（约 40%）同一个量级。
     * 用抖动 40% 的尺子量 40% 的差，结论必然落在噪声里 —— 而且会写成一个看似确定的阴性结论。
     * 只有同进程交替（ABAB）才能让预热与漂移在两臂之间均摊。
     *
     * <p>非 bench profile 下切换成 legacy 会被拒绝（409/9001）：那个开关在非压测环境里
     * 表现为"系统莫名少了 40% 吞吐"，不报错、不告警，只在某天被人当成"最近怎么变慢了"。
     */
    @PostMapping("/lua-mode")
    public R<Map<String, Object>> setLuaMode(@RequestParam boolean legacy) {
        String before = scriptRunner.isLegacyMode() ? "legacy" : "evalsha";
        try {
            scriptRunner.setLegacyMode(legacy);
        } catch (IllegalStateException e) {
            return R.fail(ErrorCode.SYSTEM_BUSY, e.getMessage());
        }
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("before", before);
        data.put("after", scriptRunner.isLegacyMode() ? "legacy" : "evalsha");
        data.put("fallbacks", scriptRunner.fallbackCount());
        return R.ok(data);
    }

    /** 读当前执行路径，不改变状态。压测脚本在每臂开始前用它确认开关真的生效了。 */
    @GetMapping("/lua-mode")
    public R<Map<String, Object>> getLuaMode() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("mode", scriptRunner.isLegacyMode() ? "legacy" : "evalsha");
        data.put("fallbacks", scriptRunner.fallbackCount());
        return R.ok(data);
    }

    /**
     * 把资源和 Redis 一起复位到基线。
     *
     * <p>顺序不能反：先改资源、再删记录、最后灌 Redis。
     * 反过来先灌 Redis 的话，中间那几十毫秒里打进来的请求会扣掉一批名额，
     * 然后被 {@code DELETE quota_record} 删掉记录 —— 于是 Redis 比 DB 少一截，
     * 而这正好是一次真实的"已扣未落库"，会把下一轮的对账数据污染掉。
     *
     * <p>清 Redis 时<b>必须</b>同时清掉这几类 key，漏一个就会出现"上一轮的残留冒充这一轮的数据"：
     * <ul>
     *   <li>{@code quota:stock:*} —— 余量本身</li>
     *   <li>{@code quota:idem:*} —— 不清的话，上一轮抢到过的人这一轮直接被判重放，永远抢不到</li>
     *   <li>{@code quota:college:*} —— 学院余量（D4）</li>
     *   <li>{@code quota:res:*} —— 资源元信息，不清会拿着旧的 total_quota 算余量（D3）</li>
     *   <li>{@code quota:roster:*} —— 名单单人视图，reset 后要按新名单重灌（D4）</li>
     * </ul>
     */
    @PostMapping("/reset")
    public R<Map<String, Object>> reset(@RequestParam long resourceId,
                                        @RequestParam(defaultValue = "0") int quota,
                                        @RequestParam(defaultValue = "false") boolean force) {
        Integer existing = jdbc.query(
                "SELECT total_quota FROM quota_resource WHERE id = ?",
                rs -> rs.next() ? rs.getInt("total_quota") : null,
                resourceId);
        if (existing == null) {
            return R.fail(ErrorCode.RESOURCE_NOT_FOUND);
        }
        // 复位之前先看一眼上一轮是否还有没落完的消息。
        // 复位之后 stock 和 remain 都被抹平，这个信息就再也读不出来了。
        Integer pendingBefore = pendingOf(resourceId);

        // 队列没排空就复位，等于把账本撕了重写：Redis 的扣减被抹平，但队列里那些消息
        // 稍后照样会被 UPDATE remain-1 落库 —— 于是 DB 比 Redis 多扣一大截，
        // pending 变成一个很大的负数（2026-10-28 实测 -27605）。
        // 这种状态不会自愈，也不会被任何判据自动纠正，只能靠下一轮 reset 覆盖。
        // 所以默认拒绝，必须显式 force 才允许"我知道账是脏的，就是要重来"。
        if (pendingBefore != null && pendingBefore != 0 && !force) {
            return R.fail(ErrorCode.SYSTEM_BUSY,
                    "复位前仍有 " + pendingBefore + " 条消息未落库（pending!=0），"
                            + "先调 /api/claim/dev/drain；确认要丢掉这部分账目请传 force=true");
        }

        int total = quota > 0 ? quota : existing;
        // 活动窗口也按应用时钟算：open_at / close_at 是 isOpen()（ClaimService）和
        // ResourceMetaCache 拿来跟"现在"比的，而后者用的是 AppTime。用 DB 的 NOW()
        // 造出来的窗口在服务器 UTC 时会整体早 8 小时，表现为"活动还没开放"。
        LocalDateTime devNow = AppTime.now();
        jdbc.update("""
                UPDATE quota_resource
                   SET total_quota = ?, remain = ?, status = 'PUBLISHED',
                       open_at = ?, close_at = ?, cancel_deadline = ?,
                       version = version + 1
                 WHERE id = ?
                """, total, total,
                devNow.minusMinutes(1), devNow.plusDays(7), devNow.plusDays(7), resourceId);
        int removed = jdbc.update("DELETE FROM quota_record WHERE resource_id = ?", resourceId);

        // 学院配额同样回到满额；没有配额行的活动这条 UPDATE 影响 0 行，无害
        int collegeRows = jdbc.update(
                "UPDATE quota_college SET remain = quota WHERE resource_id = ?", resourceId);

        // D8 起，属于「这一轮发放」的账不止 quota_record 一张表。
        // 只清 quota_record 会让下面几条判据在第二轮开始就永远为红，而且看起来像是"业务写坏了"：
        //   idempotent_log  —— 累积的幂等日志与 Redis 的 idem key 数对不上
        //   notification    —— SYS-2 每确认/释放写一条，不清则重试扫描把上一轮的一起重发
        //   quota_backfill_queue —— 补位队列残留会让「取消后是否进入补位」判断失真
        //   quota_report / reconcile_diff —— D9 的巡检输出，跨轮混在一起就分不清是哪一轮的差异
        // 注意表名是 quota_backfill_queue（不是 backfill_queue）：按实体名猜表名会在这里静默 0 行。
        int idemLogRemoved = jdbc.update("DELETE FROM idempotent_log WHERE resource_id = ?", resourceId);
        int notificationRemoved = jdbc.update("DELETE FROM notification WHERE resource_id = ?", resourceId);
        int backfillRemoved = jdbc.update("DELETE FROM quota_backfill_queue WHERE resource_id = ?", resourceId);
        int reportRemoved = jdbc.update("DELETE FROM quota_report WHERE resource_id = ?", resourceId);
        int reconcileRemoved = jdbc.update("DELETE FROM reconcile_diff WHERE resource_id = ?", resourceId);

        redis.opsForValue().set(RedisKeys.stock(resourceId), String.valueOf(total));
        int collegeKeys = seedCollegeKeys(resourceId);
        int idemCleared = deleteByPattern(RedisKeys.idem(resourceId, "") + "*");
        int rosterCleared = deleteByPattern(RedisKeys.rosterEntryPrefix(resourceId) + "*");
        int rosterLoaded = seedRosterEntries(resourceId);

        // 元信息缓存最后清：前面所有 DB 改动都做完了，下一次读一定拿到新值
        metaCache.evict(resourceId);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("resourceId", resourceId);
        data.put("totalQuota", total);
        data.put("redisRemain", total);
        data.put("deletedRecords", removed);
        // 逐张表回显删除行数：只报一个总数的话，"notification 没清"和"notification 本来就是空的"
        // 在结果里长得一模一样，而前者会让 SYS-2 的验收在第二轮莫名其妙地重复发送。
        data.put("deletedIdempotentLogs", idemLogRemoved);
        data.put("deletedNotifications", notificationRemoved);
        data.put("deletedBackfill", backfillRemoved);
        data.put("deletedReports", reportRemoved);
        data.put("deletedReconcileDiffs", reconcileRemoved);
        data.put("clearedIdemKeys", idemCleared);
        data.put("collegeRows", collegeRows);
        data.put("redisCollegeKeys", collegeKeys);
        data.put("clearedRosterKeys", rosterCleared);
        data.put("loadedRosterKeys", rosterLoaded);
        data.put("pendingBeforeReset", pendingBefore);
        if (pendingBefore != null && pendingBefore > 0) {
            data.put("warning", "上一轮还有 " + pendingBefore
                    + " 条消息未落库就被清掉了；下一轮开跑前先 /drain，否则数据会混");
        }
        return R.ok(data);
    }

    /**
     * 轻量进度探针：只回「消费到哪了」，不扫 Key 空间。
     *
     * <p>为什么要单独开一个，而不是让脚本轮询 {@code /stat}：{@code /stat} 里那句
     * {@code countByPattern(idem*)} 是 SCAN 50 万个 key，一次 1~2 秒。压测期间的
     * 收敛判定要 200ms 一次，用它轮询等于自己给自己加负载，而且会把「drain 变慢」
     * 误判成消费者的锅。
     *
     * <p>两个数就够判收敛：{@code pending} 归零 = 账平；{@code records} 停止增长
     * = 队列里没有消息在被处理了（用来兜住 pending 永远回不到 0 的情况，
     * 比如上一轮崩溃留下的重投消息或一条被误判成可重试的毒消息）。
     */
    @GetMapping("/progress")
    public R<Map<String, Object>> progress(@RequestParam long resourceId) {
        Map<String, Object> row = jdbc.query(
                "SELECT total_quota, remain FROM quota_resource WHERE id = ?",
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("totalQuota", rs.getInt("total_quota"));
                    m.put("dbRemain", rs.getInt("remain"));
                    return m;
                },
                resourceId);
        if (row == null) {
            return R.fail(ErrorCode.RESOURCE_NOT_FOUND);
        }
        int total = (Integer) row.get("totalQuota");
        int dbRemain = (Integer) row.get("dbRemain");
        Integer records = jdbc.queryForObject(
                "SELECT COUNT(*) FROM quota_record WHERE resource_id = ?", Integer.class, resourceId);
        String redisRaw = redis.opsForValue().get(RedisKeys.stock(resourceId));

        int rec = records == null ? 0 : records;
        // 与 /stat 同一口径：pending = (total - redisRemain) - (total - dbRemain) = dbRemain - redisRemain。
        // 写成这个化简式只为了让 /progress 一次 Redis GET 就够，不必再读 DB 的总名额。
        Integer pending = redisRaw == null ? null : dbRemain - Integer.parseInt(redisRaw);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("resourceId", resourceId);
        data.put("records", rec);
        data.put("dbRemain", dbRemain);
        data.put("redisRemain", redisRaw == null ? null : Integer.valueOf(redisRaw));
        data.put("pending", pending);
        return R.ok(data);
    }

    /**
     * 判据。
     *
     * <p>四个量，D3 之后缺一不可：
     * <pre>
     *   oversold  = issued - totalQuota                   带符号：正数超发，负数还没发完
     *   invariant = (dbRemain + issued == totalQuota)     唯一活得过 D8 的硬判据
     *   pending   = (total - redisRemain) - (total - dbRemain)
     *   balanced  = invariant &amp;&amp; (pending == 0) &amp;&amp; (idemKeys == records)
     * </pre>
     *
     * <p><b>为什么判据是不变式而不是"超发数"</b>：{@code oversold} 用的是
     * {@code status IN ('LOCKED','CONFIRMED')}，也就是「当前还有效的条数」。
     * D8 引入 RELEASED / CANCELLED 之后它会同时出现漏报和误报：
     * <pre>
     *   漏报：发 260 条、其中 60 条被取消 → issued = 200 → oversold = 0，超发凭空消失
     *   误报：正常发 200、取消 100、再发 100 → records = 300 → 用 records 判就会报超发
     * </pre>
     * 所以「改成用 records」不能解决它，只是把漏报换成误报——取消返还名额之后，
     * 累计 {@code records} 天然可以远超 {@code totalQuota}。
     * 真正稳定的只有 {@code dbRemain + issued == totalQuota}：取消会让 remain 加 1、
     * issued 减 1，两边反向变化，等式不动。D8 那天这条不用改。
     *
     * <p><b>{@code pending}</b> 是 D3 才有的量，也是异步化之后最需要盯的量：
     * <ul>
     *   <li>{@code pending > 0} —— 正常，消息在队列里还没落库。压测刚结束时必然如此，先 {@code /drain}</li>
     *   <li>{@code pending == 0} —— 账平了，可以下结论</li>
     *   <li>{@code pending < 0} —— <b>DB 比 Redis 多扣了</b>。只有两种成因：
     *       ①「投递超时但其实进了 Broker」导致回补与落库撞车；
     *       ② 有人手工改过 Redis。两种情况都必须出现在对账（D9）里，不能忽略</li>
     * </ul>
     *
     * <p>{@code drift = redisRemain - dbRemain} 是它的镜像（{@code drift == -pending}），
     * 两个都留着是因为 D2 的历史数据用的是 drift，改口径会让前后两组数据没法比。
     */
    @GetMapping("/stat")
    public R<Map<String, Object>> stat(@RequestParam long resourceId) {
        Map<String, Object> row = jdbc.query(
                "SELECT total_quota, remain FROM quota_resource WHERE id = ?",
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("totalQuota", rs.getInt("total_quota"));
                    m.put("dbRemain", rs.getInt("remain"));
                    return m;
                },
                resourceId);
        if (row == null) {
            return R.fail(ErrorCode.RESOURCE_NOT_FOUND);
        }

        int totalQuota = (Integer) row.get("totalQuota");
        int dbRemain = (Integer) row.get("dbRemain");

        Integer records = jdbc.queryForObject(
                "SELECT COUNT(*) FROM quota_record WHERE resource_id = ?", Integer.class, resourceId);
        Integer issued = jdbc.queryForObject(
                "SELECT COUNT(*) FROM quota_record WHERE resource_id = ? AND status IN ('LOCKED','CONFIRMED')",
                Integer.class, resourceId);

        String redisRaw = redis.opsForValue().get(RedisKeys.stock(resourceId));
        Integer redisRemain = redisRaw == null ? null : Integer.valueOf(redisRaw);

        int rec = records == null ? 0 : records;
        int iss = issued == null ? 0 : issued;
        int dbConsumed = totalQuota - dbRemain;
        Integer redisConsumed = redisRemain == null ? null : totalQuota - redisRemain;
        Integer pending = redisConsumed == null ? null : redisConsumed - dbConsumed;

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("resourceId", resourceId);
        data.put("totalQuota", totalQuota);
        data.put("dbRemain", dbRemain);
        data.put("redisRemain", redisRemain);
        data.put("drift", redisRemain == null ? null : redisRemain - dbRemain);
        data.put("records", rec);
        data.put("issued", iss);
        data.put("dbConsumed", dbConsumed);
        data.put("redisConsumed", redisConsumed);
        data.put("pending", pending);
        // oversold 输出<b>带符号</b>的 issued - totalQuota。
        // 曾经写成 Math.max(0, ...)，理由是「活动刚开始 issued=0、quota=200，
        // 差值是 -200，输出负数会让人以为负超卖是一种特殊状态」。这个理由没错，
        // 但代价太贵：clamp 之后这个字段永远不可能为负，也就永远不可能通过
        // 「数字看着不对」发现判据本身坏了。开跑前看一眼它，-200 就是「还没人抢」，
        // 这本身就是有用信息，不该被抹掉。
        data.put("oversold", iss - totalQuota);
        data.put("overflow", iss > totalQuota);

        // records 版本的超发只作<b>参考</b>，不能当判据。本轮没有取消时它与 issued 等价，
        // 所以留在这里是为了跨 D8 前后能看出「历史累计到底发了多少」。
        data.put("recordsOverQuota", rec > totalQuota);

        // 唯一的硬判据。等价于原来的 iss == dbConsumed，但写成加法形式是因为
        // 「remain + 有效记录数 == 总名额」在任何时刻都该成立，而它比
        // 「issued == 已消耗数」更能说明意图——后者看着像在自证。
        // invariantDelta 正数 = issued 偏多（超发，或 remain 被少扣）；
        // 负数 = issued 偏少（漏发，或 remain 被多扣）。方向不同，查法完全不同。
        int invariantDelta = dbRemain + iss - totalQuota;
        boolean quotaInvariant = invariantDelta == 0;
        data.put("quotaInvariant", quotaInvariant);
        data.put("invariantDelta", invariantDelta);

        // 取消还没引入（D8 之前 issued 必然等于 records）。不等就说明有状态被写坏了：
        // 要么出现了 schema 里没有的 status 值，要么有人手工改过库。
        // 这条断言在 D8 之后要跟着改，届时 issued <= records 才是恒成立的。
        data.put("issuedLeRecords", iss <= rec);

        // 幂等键数：D2 时只是个参考值，D3 之后它是<b>唯一能发现"少发"的量</b>。
        // 每成功申领一次必然写一个幂等键，所以 drain 之后必然 idemKeys == records。
        //   idemKeys > records → 有消息在队列里丢了或进了死信，或者被 DB 的余量防线挡下了。
        //                        此时 pending 看起来"还在等消费"，实际永远不会归零。
        //   idemKeys < records → 有人绕过 Lua 直接写了库（手工修数据 / 幂等键被清过）。
        // 只看 records 和 oversold 是发现不了这两件事的 —— 前者只能看到"不多发"，
        // 看不到"少发了多少"。2026-10-27 阳性对照②（Redis 灌 250、DB 仍 200）实测：
        // records=200 / oversold=0 一片太平，而 idemKeys=250 才是那 50 个"被告知抢到、
        // 实际没记录"的学生。
        int idemKeys = countByPattern(RedisKeys.idem(resourceId, "") + "*");
        data.put("idemKeys", idemKeys);
        data.put("idemMatch", idemKeys == rec);

        // 账平 = 三个维度同时成立。少任何一个都会出现「看起来平了但有人吃亏」：
        //   pending=0         → Redis 与 DB 对「发了多少」意见一致
        //   quotaInvariant    → DB 内部自洽（remain 与 issued 恰好互补），且没有超发
        //   idem==records     → 每个被认为抢到的人都有落库记录
        boolean balanced = pending != null && pending == 0 && quotaInvariant && idemKeys == rec;
        data.put("balanced", balanced);

        // 超发的<b>历史峰值</b>事后能证明、取消之后就会被抹平，快照判据看不见。
        // D8 引入取消时必须补一条定时采样（压测期间每隔 200ms 记一次 issued 取最大值），
        // 否则「发 260、取消 60、remain 回 0」这种超发在事后是完全不可见的。
        // 现在不做的唯一理由：D8 之前没有任何路径会让 issued 变小。

        List<Map<String, Object>> colleges = collegeStat(resourceId);
        if (!colleges.isEmpty()) {
            data.put("colleges", colleges);
        }
        return R.ok(data);
    }

    /**
     * 等消费者把队列里的消息落完。
     *
     * <p>压测器一跑完就查 stat，看到的 {@code pending} 一定是正数。这不是丢消息，
     * 是异步链路的正常形态。区别在于：<b>能不能等到 0</b>。
     * 等到 0 → 说明只是慢；等到超时还不动 → 才是真的有问题（消费者挂了 / 消息进了死信）。
     *
     * <p>顺带产出一个数字：<b>追平耗时</b>。它就是 D10「消费能力」实验里的
     * 「堆积多久追平」，所以这个接口不是测试脚手架，是正式观测手段。
     */
    @GetMapping("/drain")
    public R<Map<String, Object>> drain(@RequestParam long resourceId,
                                        @RequestParam(defaultValue = "30000") long timeoutMs) {
        long start = System.currentTimeMillis();
        Integer pending = null;
        boolean timedOut = false;
        while (true) {
            pending = pendingOf(resourceId);
            // 退出条件是 <= 0，不是 == 0。负数的语义不是「还没落完」，而是「DB 比 Redis
            // 多扣了」—— 它不会随着时间收敛，等下去只会白等满 timeoutMs（实测每轮 300 秒）。
            // 但退出不等于追平：drained 仍然只认 == 0，负值单独用 driftNegative 标出来，
            // 免得脚本把「账本已经脏了」读成「账平了」。
            if (pending != null && pending <= 0) {
                break;
            }
            if (System.currentTimeMillis() - start >= timeoutMs) {
                timedOut = true;
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        long cost = System.currentTimeMillis() - start;
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("resourceId", resourceId);
        data.put("pending", pending);
        data.put("drained", pending != null && pending == 0);
        data.put("driftNegative", pending != null && pending < 0);
        data.put("timedOut", timedOut);
        data.put("costMs", cost);
        return R.ok(data);
    }

    private Integer pendingOf(long resourceId) {
        Map<String, Object> row = jdbc.query(
                "SELECT total_quota, remain FROM quota_resource WHERE id = ?",
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("totalQuota", rs.getInt("total_quota"));
                    m.put("dbRemain", rs.getInt("remain"));
                    return m;
                },
                resourceId);
        if (row == null) {
            return null;
        }
        String redisRaw = redis.opsForValue().get(RedisKeys.stock(resourceId));
        if (redisRaw == null) {
            return null;
        }
        int total = (Integer) row.get("totalQuota");
        int dbConsumed = total - (Integer) row.get("dbRemain");
        int redisConsumed = total - Integer.parseInt(redisRaw);
        return redisConsumed - dbConsumed;
    }

    private List<Map<String, Object>> collegeStat(long resourceId) {
        List<Map<String, Object>> rows = jdbc.query(
                "SELECT college_code, quota, remain FROM quota_college WHERE resource_id = ? ORDER BY college_code",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("college", rs.getString("college_code"));
                    m.put("quota", rs.getInt("quota"));
                    m.put("dbRemain", rs.getInt("remain"));
                    return m;
                },
                resourceId);
        for (Map<String, Object> m : rows) {
            String code = (String) m.get("college");
            String raw = redis.opsForValue().get(RedisKeys.collegeStock(resourceId, code));
            m.put("redisRemain", raw == null ? null : Integer.valueOf(raw));
        }
        return rows;
    }

    /** 把 quota_college 的 remain 灌进 Redis，返回写入的 key 数。 */
    private int seedCollegeKeys(long resourceId) {
        List<Map<String, Object>> rows = jdbc.query(
                "SELECT college_code, remain FROM quota_college WHERE resource_id = ?",
                (rs, i) -> Map.<String, Object>of(
                        "code", rs.getString("college_code"),
                        "remain", rs.getInt("remain")),
                resourceId);
        for (Map<String, Object> row : rows) {
            redis.opsForValue().set(
                    RedisKeys.collegeStock(resourceId, (String) row.get("code")),
                    String.valueOf(row.get("remain")));
        }
        return rows.size();
    }

    /**
     * 把资格名单灌成「单人一个 key」的学院视图（D4 的学院配额靠它）。
     *
     * <p>用 pipeline 而不是逐条 SET：3 万人的名单逐条往返要好几秒，
     * 而 reset 是每轮压测前都要跑的动作，慢一点都会被嫌。
     */
    private int seedRosterEntries(long resourceId) {
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        jdbc.query("SELECT student_id, college_code FROM quota_roster WHERE resource_id = ?",
                rs -> {
                    String college = rs.getString("college_code");
                    if (college != null && !college.isBlank()) {
                        entries.add(Map.entry(rs.getString("student_id"), college));
                    }
                },
                resourceId);
        if (entries.isEmpty()) {
            return 0;
        }
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {
                RedisOperations<String, String> ops = (RedisOperations<String, String>) operations;
                for (Map.Entry<String, String> e : entries) {
                    ops.opsForValue().set(RedisKeys.rosterEntry(resourceId, e.getKey()), e.getValue());
                }
                return null;
            }
        });
        return entries.size();
    }

    /**
     * 按 pattern 数 key 个数（不删）。
     *
     * <p>用 SCAN 游标而不是 {@code DBSIZE} 或 {@code KEYS}：DBSIZE 是整库的，
     * KEYS 会阻塞 Redis。这个接口是 dev 专用，慢一点无所谓，但不能影响正在跑的链路。
     */
    /**
     * 设置通知通道的失败注入（SYS-2 验收用）。
     *
     * <p><b>语义是"前 N 次尝试一律失败"，不是概率。</b>概率注入会把"最终全部 SENT"
     * 变成一个只有六成概率成立的事件（每条 3 次全失败概率 (1-p)^3，50 条里至少一条进 DEAD
     * 的概率接近 1），验收就会"一半概率红"，而人看到红灯只会去查代码。
     * {@code failAttempts=0} 全成功、{@code =1} 每条失败一次后成功、
     * {@code >=maxRetry} 全部进 DEAD —— 三种终态都能稳定复现。
     *
     * <p>运行时改而不用改配置重启：验收要在一个 JVM 里连跑三种注入，
     * 重启会把"冷启动慢"混进数字里（D8 阶段 0 的教训）。
     */
    @PostMapping("/notify-mock")
    public R<Map<String, Object>> setNotifyMock(@RequestParam int failAttempts) {
        int before = mockNotifySender.failAttempts();
        mockNotifySender.setFailAttempts(failAttempts);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("before", before);
        data.put("after", mockNotifySender.failAttempts());
        return R.ok(data);
    }

    /** 读当前注入值，不改变状态。 */
    @GetMapping("/notify-mock")
    public R<Map<String, Object>> getNotifyMock() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("failAttempts", mockNotifySender.failAttempts());
        return R.ok(data);
    }

    /**
     * 通知判据：按状态计数 + 是否出现重复 bizId + 前 50 行明细。
     *
     * <p>{@code total == distinctBiz} 是"唯一键真的挡住了重投"的判据。
     * 少了这一条，重投造成重复通知这件事只有靠读代码才知道。
     */
    @GetMapping("/notify")
    public R<Map<String, Object>> notifyStat(@RequestParam long resourceId) {
        Integer total = jdbc.queryForObject(
                "SELECT COUNT(*) FROM notification WHERE resource_id = ?", Integer.class, resourceId);
        Integer distinctBiz = jdbc.queryForObject(
                "SELECT COUNT(DISTINCT biz_id) FROM notification WHERE resource_id = ?",
                Integer.class, resourceId);
        List<Map<String, Object>> counts = notificationMapper.countByStatus(resourceId);
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT id, biz_id, channel, status, retry_count, next_retry_at, sent_at, error_msg
                  FROM notification WHERE resource_id = ? ORDER BY id LIMIT 50
                """, resourceId);

        int t = total == null ? 0 : total;
        int d = distinctBiz == null ? 0 : distinctBiz;
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("total", t);
        data.put("distinctBiz", d);
        data.put("duplicateBiz", t - d);
        data.put("byStatus", counts);
        data.put("rows", rows);
        return R.ok(data);
    }

    private int countByPattern(String pattern) {
        int count = 0;
        try (Cursor<String> cursor = redis.scan(
                ScanOptions.scanOptions().match(pattern).count(1000).build())) {
            while (cursor.hasNext()) {
                cursor.next();
                count++;
            }
        }
        return count;
    }

    /**
     * 按 pattern 删 key。
     *
     * <p>用 SCAN 而不是 {@code KEYS quota:idem:1:*}：KEYS 会阻塞整个 Redis，
     * 在压测间隙清一次就能让下一轮的首个请求卡住几百毫秒，
     * 这个毛刺会直接体现在 P99 上，而你会以为是 Redis 性能问题。
     */
    private int deleteByPattern(String pattern) {
        Set<String> keys = new HashSet<>();
        try (Cursor<String> cursor = redis.scan(
                ScanOptions.scanOptions().match(pattern).count(1000).build())) {
            cursor.forEachRemaining(keys::add);
        }
        if (keys.isEmpty()) {
            return 0;
        }
        Long removed = redis.delete(keys);
        return removed == null ? 0 : removed.intValue();
    }
}
