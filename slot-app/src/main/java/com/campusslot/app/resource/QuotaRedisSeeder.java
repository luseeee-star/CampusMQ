package com.campusslot.app.resource;

import com.campusslot.common.constant.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 把活动配置灌进 Redis：余量、各学院余量、资格名单（三类 key）。
 *
 * <p><b>为什么单独一个类，而不是留在发布逻辑里</b>：灌名单这段（pipeline、跳过空学院编码、
 * 单人一个 key）在 dev 的 {@code /reset} 里已经有一份，发布要用同一套。复制一份的代价是
 * 两边迟早只改一处——最典型的是"发布走覆盖、reset 走只补缺"这种差异，复制之后
 * 只会在某次实验里表现成"reset 之后名单还是旧的"。
 *
 * <p><b>{@code onlyIfAbsent} 的语义</b>（发布的两个分支，见计划书 D11）：
 * <ul>
 *   <li>{@code false} = 覆盖写。用在 DRAFT 发布（未上线、无并发，覆盖正是为了校正上一次的残留）
 *       和 dev reset（把上一轮的残留拉回基线）。</li>
 *   <li>{@code true} = 只补缺（{@code SETNX}）。用在 PUBLISHED 重跑：有人在抢，
 *       绝不能拿数据库的 remain 覆盖掉 Redis 里正在被扣的值。</li>
 * </ul>
 * 写成"一律 SETNX"会留一个洞：上一次灌了 3000、改状态前进程崩了，活动还是 DRAFT；
 * 运营把名额改成 2000（remain 一起改），再发布时 SETNX 发现 key 存在，一个都不动，
 * 状态迁移成功，凭空多出 1000 个名额。
 *
 * <p><b>三类 key 都不设 TTL</b>：key 缺失 = 系统繁忙（{@code claim.lua} 返回 -3），
 * 不能让它们自己消失。一个 key 几十字节，几万个活动也就几 MB。
 */
@Component
public class QuotaRedisSeeder {

    private static final Logger log = LoggerFactory.getLogger(QuotaRedisSeeder.class);

    /** 单批 pipeline 的命令数。三万人的名单一次塞进去会在客户端攒出几十 MB 的缓冲。 */
    private static final int CHUNK = 1000;

    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;

    public QuotaRedisSeeder(StringRedisTemplate redis, JdbcTemplate jdbc) {
        this.redis = redis;
        this.jdbc = jdbc;
    }

    /**
     * 总余量。返回值：覆盖写恒为 1；只补缺时 1 = 这次补上了，0 = key 已存在没动。
     *
     * <p><b>值必须由调用方传数据库的 {@code remain}</b>，不能在这里自己读 {@code total_quota}：
     * PUBLISHED 状态下 remain 才是真实余量，用 total 回填等于把已经发出去的又加回去。
     */
    public int seedStock(long resourceId, int remain, boolean onlyIfAbsent) {
        String key = RedisKeys.stock(resourceId);
        String value = String.valueOf(remain);
        if (!onlyIfAbsent) {
            redis.opsForValue().set(key, value);
            return 1;
        }
        return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key, value)) ? 1 : 0;
    }

    /** 各学院余量，值取该行自己的 {@code remain}。没有学院行时写 0 个 key。 */
    public int seedColleges(long resourceId, boolean onlyIfAbsent) {
        List<Map<String, Object>> rows = jdbc.query(
                "SELECT college_code, remain FROM quota_college WHERE resource_id = ?",
                (rs, i) -> Map.<String, Object>of(
                        "code", rs.getString("college_code"),
                        "remain", rs.getInt("remain")),
                resourceId);
        if (rows.isEmpty()) {
            return 0;
        }
        List<String> keys = new ArrayList<>(rows.size());
        List<String> values = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            keys.add(RedisKeys.collegeStock(resourceId, (String) row.get("code")));
            values.add(String.valueOf(row.get("remain")));
        }
        return pipeSet(keys, values, onlyIfAbsent);
    }

    /**
     * 资格名单灌成「单人一个 key」的学院视图。
     *
     * <p><b>为什么不是整份名单一个 Hash</b>：Hash 在 Cluster 里是单个 slot，
     * 三万人的名单会变成热点 key，所有 API 机器打在同一个分片上；单人视图是 O(1) 且天然分散。
     *
     * <p>学院编码为空的行会被跳过（{@code quota_roster.college_code DEFAULT NULL}）。
     * 跳过是<b>静默</b>的：整份名单如果都没填编码，灌完 0 个 key、发布照样成功、
     * 抢的时候全场 1005。所以这里要把跳过数打成 WARN——建活动接口已经强制了必填，
     * 还能走到这里的只可能是手工 SQL 插进来的数据，那正是最需要留痕的情况。
     */
    public int seedRoster(long resourceId, boolean onlyIfAbsent) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        int[] skipped = {0};
        jdbc.query("SELECT student_id, college_code FROM quota_roster WHERE resource_id = ?",
                rs -> {
                    String college = rs.getString("college_code");
                    if (college == null || college.isBlank()) {
                        skipped[0]++;
                        return;
                    }
                    keys.add(RedisKeys.rosterEntry(resourceId, rs.getString("student_id")));
                    values.add(college);
                },
                resourceId);
        if (skipped[0] > 0) {
            log.warn("P0 名单有 {} 行没有学院编码，已被跳过；这些学生会被判成不在名单内 resourceId={}",
                    skipped[0], resourceId);
        }
        if (keys.isEmpty()) {
            return 0;
        }
        return pipeSet(keys, values, onlyIfAbsent);
    }

    /** 名单总行数（含没有学院编码的行）。发布前用它挡「名单写着要、实际是空的」。 */
    public int countRoster(long resourceId) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM quota_roster WHERE resource_id = ?", Integer.class, resourceId);
        return n == null ? 0 : n;
    }

    /**
     * 分批 pipeline 写。返回"真正写进去的条数"：
     * 覆盖写 = 全部；只补缺 = 只有原本不存在的那些（{@code SETNX} 返回 1 的）。
     */
    private int pipeSet(List<String> keys, List<String> values, boolean onlyIfAbsent) {
        int written = 0;
        for (int from = 0; from < keys.size(); from += CHUNK) {
            final int start = from;
            final int end = Math.min(from + CHUNK, keys.size());
            List<Object> results = redis.executePipelined(new SessionCallback<Object>() {
                @Override
                @SuppressWarnings("unchecked")
                public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {
                    RedisOperations<String, String> ops = (RedisOperations<String, String>) operations;
                    for (int i = start; i < end; i++) {
                        if (onlyIfAbsent) {
                            ops.opsForValue().setIfAbsent(keys.get(i), values.get(i));
                        } else {
                            ops.opsForValue().set(keys.get(i), values.get(i));
                        }
                    }
                    return null;
                }
            });
            if (onlyIfAbsent) {
                for (Object r : results) {
                    if (Boolean.TRUE.equals(r)) {
                        written++;
                    }
                }
            } else {
                written += end - start;
            }
        }
        return written;
    }
}
