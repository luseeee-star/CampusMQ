package com.campusmq.broker.consume;

import com.campusmq.broker.config.BrokerConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费进度（执行方案 §3.5）：按 (topic, group, queueId) 三元组记录，
 * 每 5 秒持久化到 JSON 文件。
 *
 * 语义：先执行业务再提交进度 = 至少一次。极端情况下（持久化间隔内宕机）
 * 会重复消费一小段——这是 §12.2 Q4 主动承认的取舍。
 */
public class ConsumerOffsetManager {

    private static final Logger log = LoggerFactory.getLogger(ConsumerOffsetManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BrokerConfig config;
    private final Map<String, AtomicLong> offsets = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "offset-persist");
        t.setDaemon(true);
        return t;
    });

    public ConsumerOffsetManager(BrokerConfig config) {
        this.config = config;
    }

    public static String key(String topic, String group, int queueId) {
        return topic + "@" + group + "@" + queueId;
    }

    public void load() {
        File file = offsetFile();
        if (!file.exists()) {
            return;
        }
        try {
            List<OffsetEntry> entries = MAPPER.readValue(file, new TypeReference<>() {});
            for (OffsetEntry e : entries) {
                offsets.put(key(e.topic, e.group, e.queueId), new AtomicLong(e.offset));
            }
            log.info("consumer offsets loaded, {} entries", offsets.size());
        } catch (Exception e) {
            log.warn("load consumer offsets failed, start empty", e);
        }
    }

    public long queryOffset(String topic, String group, int queueId) {
        AtomicLong v = offsets.get(key(topic, group, queueId));
        return v == null ? 0 : v.get();
    }

    /** 只前进不回退，避免旧值覆盖新值（§7 并发清单） */
    public void commitOffset(String topic, String group, int queueId, long offset) {
        offsets.computeIfAbsent(key(topic, group, queueId), k -> new AtomicLong(0))
                .updateAndGet(cur -> Math.max(cur, offset));
    }

    public void start() {
        long interval = config.getOffsetPersistIntervalMs();
        scheduler.scheduleWithFixedDelay(this::persist, interval, interval, TimeUnit.MILLISECONDS);
    }

    public synchronized void persist() {
        List<OffsetEntry> entries = new ArrayList<>();
        offsets.forEach((k, v) -> {
            String[] parts = k.split("@");
            OffsetEntry e = new OffsetEntry();
            e.topic = parts[0];
            e.group = parts[1];
            e.queueId = Integer.parseInt(parts[2]);
            e.offset = v.get();
            e.ts = System.currentTimeMillis();
            entries.add(e);
        });
        try {
            File file = offsetFile();
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(file, entries);
        } catch (Exception e) {
            log.error("persist consumer offsets failed", e);
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        persist();
    }

    private File offsetFile() {
        return new File(config.getStorePath() + File.separator + "offset"
                + File.separator + "offsets.json");
    }

    /** 序列化条目，字段与执行方案 §3.5 示例一致 */
    public static class OffsetEntry {
        public String topic;
        public String group;
        public int queueId;
        public long offset;
        public long ts;
    }
}
