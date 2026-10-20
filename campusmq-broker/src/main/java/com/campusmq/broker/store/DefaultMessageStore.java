package com.campusmq.broker.store;

import com.campusmq.broker.config.BrokerConfig;
import com.campusmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 存储层统一入口（执行方案 §2.1 包结构）。
 * 组装：CommitLog / ConsumeQueue / ReputMessageService / FlushCommitLogService / StoreCheckpoint。
 */
public class DefaultMessageStore {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessageStore.class);

    private final BrokerConfig config;
    private final CommitLog commitLog;
    private final Map<String, ConsumeQueue> consumeQueues = new ConcurrentHashMap<>();
    private final ReputMessageService reputService;
    private final StoreCheckpoint checkpoint;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "store-scheduler");
        t.setDaemon(true);
        return t;
    });

    public DefaultMessageStore(BrokerConfig config) {
        this.config = config;
        this.commitLog = new CommitLog(config.getStorePath(), config.getCommitLogMappedFileSize());
        this.checkpoint = new StoreCheckpoint(config.getStorePath());
        this.reputService = new ReputMessageService(commitLog, this::findOrCreateConsumeQueue);
    }

    public void load() {
        commitLog.load();
        loadConsumeQueues();
        checkpoint.load();
        // 1. 恢复 CommitLog 尾部（丢弃坏尾）
        long validEnd = commitLog.recover(checkpoint.getCommitlogMaxFlushedOffset());
        // 2. reput 位点不能超过 commitlog 有效末尾（坏尾被截断时回退）
        long reputOffset = Math.min(checkpoint.getConsumeQueueMaxBuiltOffset(), validEnd);
        // 3. 用 ConsumeQueue 已建索引回填各队列的下一个 queueOffset
        consumeQueues.values().forEach(cq ->
                commitLog.setQueueOffset(cq.getTopic(), cq.getQueueId(), cq.getMaxOffsetInQueue()));
        log.info("message store loaded, validEnd={}, reputFrom={}", validEnd, reputOffset);
        this.reputStartOffset = reputOffset;
    }

    private long reputStartOffset;

    private void loadConsumeQueues() {
        File dir = new File(config.getStorePath() + File.separator + "consumequeue");
        File[] topicDirs = dir.listFiles();
        if (topicDirs == null) {
            return;
        }
        for (File topicDir : topicDirs) {
            File[] queueDirs = topicDir.listFiles();
            if (queueDirs == null) {
                continue;
            }
            for (File queueDir : queueDirs) {
                ConsumeQueue cq = new ConsumeQueue(topicDir.getName(), Integer.parseInt(queueDir.getName()),
                        config.getStorePath(), config.getConsumeQueueMappedFileSize());
                cq.load();
                consumeQueues.put(CommitLog.queueKey(cq.getTopic(), cq.getQueueId()), cq);
            }
        }
    }

    public void start() {
        reputService.start(reputStartOffset);
        if (config.getFlushType() != BrokerConfig.FlushType.SYNC) {
            long interval = config.getFlushType().intervalMs();
            scheduler.scheduleWithFixedDelay(this::flushAll, interval, interval, TimeUnit.MILLISECONDS);
        }
        // checkpoint 每秒落盘一次（LOGBOOK 教训：刷太慢会导致重启后重复建索引）
        scheduler.scheduleWithFixedDelay(this::saveCheckpoint, 1, 1, TimeUnit.SECONDS);
    }

    /** 写入入口：追加 CommitLog；同步刷盘策略下 force 后才返回。 */
    public AppendMessageResult putMessage(MessageExt msg) {
        AppendMessageResult result = commitLog.putMessage(msg);
        if (config.getFlushType() == BrokerConfig.FlushType.SYNC) {
            commitLog.flush();
            checkpoint.setCommitlogMaxFlushedOffset(commitLog.getMaxOffset());
        }
        return result;
    }

    /** 按逻辑位点拉取消息。返回 [offset, offset+maxNum) 区间实际可读到的消息。 */
    public List<MessageExt> getMessage(String topic, int queueId, long offset, int maxNum) {
        List<MessageExt> result = new ArrayList<>();
        ConsumeQueue cq = consumeQueues.get(CommitLog.queueKey(topic, queueId));
        if (cq == null) {
            return result;
        }
        long maxOffset = cq.getMaxOffsetInQueue();
        long now = System.currentTimeMillis();
        for (long i = offset; i < Math.min(offset + maxNum, maxOffset); i++) {
            ByteBuffer index = cq.getIndexBuffer(i);
            if (index == null) {
                break;
            }
            long commitlogOffset = index.getLong();
            index.getInt(); // msgSize，CommitLog 自己会再解析
            long deliverTimestamp = index.getLong(); // tagsHash 槽位复用
            if (deliverTimestamp > now) {
                // 延迟消息未到期，不可见（D7 链路后才会出现这种条目）
                break;
            }
            MessageExt msg = commitLog.getMessage(commitlogOffset);
            if (msg != null) {
                result.add(msg);
            }
        }
        return result;
    }

    public long getMaxQueueOffset(String topic, int queueId) {
        ConsumeQueue cq = consumeQueues.get(CommitLog.queueKey(topic, queueId));
        return cq == null ? 0 : cq.getMaxOffsetInQueue();
    }

    private ConsumeQueue findOrCreateConsumeQueue(String topic, int queueId) {
        return consumeQueues.computeIfAbsent(CommitLog.queueKey(topic, queueId), k -> {
            ConsumeQueue cq = new ConsumeQueue(topic, queueId, config.getStorePath(),
                    config.getConsumeQueueMappedFileSize());
            cq.load();
            return cq;
        });
    }

    private void flushAll() {
        commitLog.flush();
        consumeQueues.values().forEach(ConsumeQueue::flush);
        checkpoint.setCommitlogMaxFlushedOffset(commitLog.getMaxOffset());
    }

    private void saveCheckpoint() {
        checkpoint.setConsumeQueueMaxBuiltOffset(reputService.getReputFromOffset());
        checkpoint.save();
    }

    public void shutdown() {
        scheduler.shutdown();
        reputService.shutdown();
        flushAll();
        saveCheckpoint();
        consumeQueues.values().forEach(ConsumeQueue::shutdown);
        commitLog.shutdown();
        log.info("message store shutdown");
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public StoreCheckpoint getCheckpoint() {
        return checkpoint;
    }
}
