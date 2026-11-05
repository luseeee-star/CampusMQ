package com.campusslot.app.controller;

import com.campusslot.common.api.R;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.mq.MqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * 环境自检面板的数据源。
 *
 * <p>存在的理由：环境问题的排查成本极高，而「对账永远对不平」「申领全部失败但不报错」
 * 这类症状的根因往往只是一个容器没起、一个时区没设、一个 maxmemory-policy 配错。
 * 把三个中间件的连通性与关键配置一次性暴露出来，比逐个 docker exec 试错快得多。
 */
@RestController
@RequestMapping("/api/health")
public class EnvHealthController {

    private static final Logger log = LoggerFactory.getLogger(EnvHealthController.class);

    private final DataSource dataSource;
    private final StringRedisTemplate redisTemplate;
    private final MqProperties mqProperties;

    public EnvHealthController(DataSource dataSource,
                               StringRedisTemplate redisTemplate,
                               MqProperties mqProperties) {
        this.dataSource = dataSource;
        this.redisTemplate = redisTemplate;
        this.mqProperties = mqProperties;
    }

    @GetMapping("/env")
    public R<Map<String, Object>> env() {
        Map<String, Object> result = new LinkedHashMap<>();
        // 应用时钟与 dbNow 并排展示：两者差 8 小时就是时区不一致，肉眼可见，
        // 不用等到"活动没开放"或"确认全超时"才发现。
        result.put("time", AppTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        result.put("timezone", TimeZone.getDefault().getID());
        result.put("javaVersion", System.getProperty("java.version"));
        result.put("mysql", checkMysql());
        result.put("redis", checkRedis());
        result.put("rocketmq", checkRocketMq());
        return R.ok(result);
    }

    private Map<String, Object> checkMysql() {
        Map<String, Object> item = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection()) {
            item.put("ok", conn.isValid(2));
            item.put("product", conn.getMetaData().getDatabaseProductVersion());
            // 时区错 8 小时的表现是「对账在错误的时间段比数据，永远对不平」
            // 这里的 NOW() 是**故意保留的唯一一处**：它就是要读服务器时钟，好和上面的
            // AppTime.now() 并排比对。业务 SQL 里一律传参（见 QuotaRecordMapper 的注释），
            // 不要把这一处当成"还有 NOW() 没改完"。
            try (var st = conn.createStatement();
                 var rs = st.executeQuery("SELECT @@global.time_zone, @@session.time_zone, NOW()")) {
                if (rs.next()) {
                    item.put("globalTimeZone", rs.getString(1));
                    item.put("sessionTimeZone", rs.getString(2));
                    item.put("dbNow", rs.getString(3));
                }
            }
        } catch (Exception e) {
            log.debug("mysql health check failed", e);
            item.put("ok", false);
            item.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return item;
    }

    private Map<String, Object> checkRedis() {
        Map<String, Object> item = new LinkedHashMap<>();
        try {
            String probeKey = "health:probe";
            redisTemplate.opsForValue().set(probeKey, "1");
            item.put("ok", "1".equals(redisTemplate.opsForValue().get(probeKey)));
            redisTemplate.delete(probeKey);

            Properties memory = redisTemplate.execute((RedisCallback<Properties>) c ->
                    c.serverCommands().info("memory"));
            if (memory != null) {
                item.put("usedMemory", memory.getProperty("used_memory_human", "-"));
                item.put("maxMemory", memory.getProperty("maxmemory_human", "-"));
            }

            // 库存 key 被 LRU 淘汰 → Lua 读到 nil → 表现为「全部申领失败但不报错」
            Properties config = redisTemplate.execute((RedisCallback<Properties>) c ->
                    c.serverCommands().getConfig("maxmemory-policy"));
            item.put("maxmemoryPolicy", config == null ? "unknown" : config.getProperty("maxmemory-policy", "unknown"));
        } catch (Exception e) {
            log.debug("redis health check failed", e);
            item.put("ok", false);
            item.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return item;
    }

    private Map<String, Object> checkRocketMq() {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("enabled", mqProperties.isEnabled());
        item.put("endpoints", mqProperties.getEndpoints());
        item.put("note", mqProperties.isEnabled()
                ? "已启用：消息会真正发往 Broker"
                : "未启用：主链路只打日志不落 Broker，严禁在此状态下采信压测数据");
        return item;
    }
}
