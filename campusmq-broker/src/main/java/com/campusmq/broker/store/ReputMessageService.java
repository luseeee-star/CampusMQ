package com.campusmq.broker.store;

import com.campusmq.common.message.MessageExt;
import com.campusmq.common.message.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

/**
 * 异步建索引线程（执行方案 §2 架构图）：
 * 从 reputFromOffset 顺序扫描 CommitLog，把每条消息分发到对应 ConsumeQueue。
 * 位点用 volatile long 单线程推进、多读免锁（§7 并发清单）。
 */
public class ReputMessageService {

    private static final Logger log = LoggerFactory.getLogger(ReputMessageService.class);

    private final CommitLog commitLog;
    /** (topic, queueId) -> ConsumeQueue，由 DefaultMessageStore 提供 */
    private final BiFunction<String, Integer, ConsumeQueue> consumeQueueLoader;

    private volatile long reputFromOffset = 0;
    private volatile boolean running = false;
    private Thread thread;

    public ReputMessageService(CommitLog commitLog, BiFunction<String, Integer, ConsumeQueue> consumeQueueLoader) {
        this.commitLog = commitLog;
        this.consumeQueueLoader = consumeQueueLoader;
    }

    public void start(long fromOffset) {
        this.reputFromOffset = fromOffset;
        this.running = true;
        this.thread = new Thread(this::run, "reput-message-service");
        this.thread.setDaemon(true);
        this.thread.start();
        log.info("reput service started, fromOffset={}", fromOffset);
    }

    private void run() {
        while (running) {
            try {
                doReput();
            } catch (Exception e) {
                log.error("reput error, retry after 1s", e);
                sleep(1000);
            }
        }
    }

    private void doReput() {
        CommitLog.DataSlice slice = commitLog.getData(reputFromOffset);
        if (slice == null) {
            sleep(10);
            return;
        }
        // 空洞跳跃后真实起点可能领先于 reputFromOffset
        long cursor = slice.startOffset;
        ByteBuffer data = slice.buffer;
        boolean advanced = false;
        while (data.hasRemaining()) {
            int before = data.position();
            MessageExt msg;
            try {
                msg = MessageSerializer.decode(data);
            } catch (MessageSerializer.CorruptMessageException e) {
                log.error("reput meet corrupt message at {}, stop", cursor, e);
                return;
            }
            if (msg == null) {
                break; // 半条，等下次写入后重扫
            }
            int msgSize = data.position() - before;

            ConsumeQueue cq = consumeQueueLoader.apply(msg.getTopic(), msg.getQueueId());
            // tagsHash 槽位复用存投递时刻（§3.3）
            cq.putMessagePositionInfo(cursor, msgSize, msg.getDeliverTimestamp());
            cursor += msgSize;
            reputFromOffset = cursor;
            advanced = true;
        }
        if (!advanced) {
            sleep(10);
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        running = false;
        if (thread != null) {
            try {
                thread.join(3000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public long getReputFromOffset() {
        return reputFromOffset;
    }
}
