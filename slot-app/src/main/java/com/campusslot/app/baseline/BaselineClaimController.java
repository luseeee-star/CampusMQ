package com.campusslot.app.baseline;

import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.api.R;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * D1 对照组：故意会超卖的裸数据库版本。
 *
 * <p><b>这个类的代码是「错」的，而且必须保持是错的。</b> 它是 D2 引入 Redis 之后的对照基线，
 * 用来回答「不加 Redis，200 并发下到底会超卖多少件」。
 * 看到这里的人请注意：不要"顺手修好"它，修好了 D1 的实验就没了。
 *
 * <p>正确版本在 {@code com.campusslot.app.claim}，那边的判定只有一次，
 * 在 Redis 的一个 Lua 脚本里完成（需求书 §1 硬规则 2）。
 *
 * <p>这里刻意保留的三处错误，每一处都对应后面某一层要解决的问题：
 * <table border="1">
 *   <caption>对照关系</caption>
 *   <tr><th>错误</th><th>由谁解决</th></tr>
 *   <tr><td>先 SELECT 再 UPDATE，中间有竞态窗口</td><td>D2 Redis 原子扣减</td></tr>
 *   <tr><td>UPDATE 不带 {@code WHERE remain > 0}，也不看受影响行数</td><td>D2 / D3 数据库兜底</td></tr>
 *   <tr><td>扣减与落库在同一个同步链路里，用户要等两次写</td><td>D3 MQ 异步落库</td></tr>
 * </table>
 *
 * <p>只有 {@code campusslot.dev-endpoints=true} 时才装配，试点上线前必须关掉。
 */
@RestController
@RequestMapping("/api/baseline")
@ConditionalOnProperty(name = "campusslot.dev-endpoints", havingValue = "true")
public class BaselineClaimController {

    private final JdbcTemplate jdbc;

    public BaselineClaimController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 裸版抢名额：三步分离、无事务、无锁。
     *
     * <p>超卖的根源在第二步到第三步之间：两个请求可能读到同一个 remain=1，
     * 都判断"还有余量"，然后各自执行一次减一。最终 remain 变成 -1，
     * 甚至 -37 —— 负数就是超卖的直接证据。
     */
    @PostMapping("/claim")
    public R<Map<String, Object>> claim(@RequestParam long activityId,
                                        @RequestParam String studentId) {
        if (studentId == null || studentId.isBlank()) {
            return R.fail(ErrorCode.PARAM_INVALID, "studentId 不能为空");
        }

        // 第 1 步：读余量
        Integer remain = jdbc.query(
                "SELECT remain FROM bare_activity WHERE id = ?",
                rs -> rs.next() ? rs.getInt("remain") : null,
                activityId);
        if (remain == null) {
            return R.fail(ErrorCode.RESOURCE_NOT_FOUND);
        }

        // 第 2 步：判断 —— 竞态窗口就在这个判断和下面的 UPDATE 之间
        if (remain <= 0) {
            return R.fail(ErrorCode.QUOTA_FULL);
        }

        // 第 3 步：写。注意这里没有 WHERE remain > 0，也没有检查返回值，
        // 因为裸版就是要让大家看见"数据库一句约束都不加会发生什么"。
        jdbc.update("UPDATE bare_activity SET remain = remain - 1 WHERE id = ?", activityId);
        jdbc.update("INSERT INTO bare_slot_record (activity_id, student_id) VALUES (?, ?)",
                activityId, studentId);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("result", "LOCKED");
        data.put("remain", remain - 1);
        return R.ok(data);
    }

    /**
     * 重置到基线，便于同一台机器反复跑多轮对照。
     *
     * <p>每轮压测前必须调用，否则上一轮的 remain 负值和记录会污染下一轮数据。
     */
    @PostMapping("/reset")
    public R<Map<String, Object>> reset(@RequestParam long activityId,
                                        @RequestParam(defaultValue = "0") int quota) {
        int total;
        if (quota > 0) {
            jdbc.update("UPDATE bare_activity SET total_quota = ?, remain = ? WHERE id = ?",
                    quota, quota, activityId);
            total = quota;
        } else {
            Integer t = jdbc.query(
                    "SELECT total_quota FROM bare_activity WHERE id = ?",
                    rs -> rs.next() ? rs.getInt("total_quota") : null,
                    activityId);
            if (t == null) {
                return R.fail(ErrorCode.RESOURCE_NOT_FOUND);
            }
            jdbc.update("UPDATE bare_activity SET remain = total_quota WHERE id = ?", activityId);
            total = t;
        }
        jdbc.update("DELETE FROM bare_slot_record WHERE activity_id = ?", activityId);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("activityId", activityId);
        data.put("totalQuota", total);
        data.put("records", 0);
        return R.ok(data);
    }

    /**
     * 实验判据。只看「成功数」是没有意义的 —— 那只能说明接口返回了 N 次成功，
     * 不能说明账是平的。
     *
     * <pre>
     *   records  = 落库的申领记录数
     *   remain   = 余量，为负即超卖
     *   oversold = records - total_quota      &gt; 0 即超卖
     * </pre>
     *
     * <p>判据只有一条：{@code remain < 0}。{@code records} 与 {@code consumed}
     * 恒等（成功一次就是「减一」+「插一条」成对执行），所以它们不是两个独立校验 ——
     * 2026-10-27 实测后修正了原先「三个数互不相等」的说法。
     *
     * <p>另外，零超卖也可能是假的：请求总数没超过名额时永远碰不到边界。
     * 压测前先确认 {@code 并发 × 次数 > total_quota}。
     */
    @GetMapping("/stat")
    public R<Map<String, Object>> stat(@RequestParam long activityId) {
        Map<String, Object> row = jdbc.query(
                "SELECT total_quota, remain FROM bare_activity WHERE id = ?",
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("totalQuota", rs.getInt("total_quota"));
                    m.put("remain", rs.getInt("remain"));
                    return m;
                },
                activityId);
        if (row == null) {
            return R.fail(ErrorCode.RESOURCE_NOT_FOUND);
        }

        Integer records = jdbc.queryForObject(
                "SELECT COUNT(*) FROM bare_slot_record WHERE activity_id = ?",
                Integer.class, activityId);

        int totalQuota = (Integer) row.get("totalQuota");
        int remain = (Integer) row.get("remain");

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("activityId", activityId);
        data.put("totalQuota", totalQuota);
        data.put("remain", remain);
        data.put("records", records);
        data.put("consumed", totalQuota - remain);
        data.put("oversold", records == null ? 0 : records - totalQuota);
        data.put("spurious", remain < 0);
        return R.ok(data);
    }
}
