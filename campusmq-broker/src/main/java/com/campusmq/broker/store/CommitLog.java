package com.campusmq.broker.store;

import com.campusmq.common.message.MessageExt;
import com.campusmq.common.message.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息主体存储：所有 topic 顺序追加写同一组文件。
 *
 * 恢复算法（执行方案 §3.4）：
 * 从 checkpoint 的 commitlogMaxFlushedOffset 开始正向扫描，逐条按 msgSize 跳跃、
 * 校验 magicCode 与 bodyCRC，遇到任一项失败即停止并把该位置设为文件末尾
 * （丢弃尾部半条——"最终一致 + 丢弃尾部"而不是"全有或全无"）。
 */
public class CommitLog {

    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    private final MappedFileQueue mappedFileQueue;
    /** topic@queueId -> 下一个 queueOffset，写锁内分配 */
    private final Map<String, AtomicLong> queueOffsets = new ConcurrentHashMap<>();

    public CommitLog(String storePath, int mappedFileSize) {
        this.mappedFileQueue = new MappedFileQueue(storePath + java.io.File.separator + "commitlog", mappedFileSize);
    }

    public void load() {
        mappedFileQueue.load();
    }

    /** 追加一条消息。写锁内分配 queueOffset，保证同一队列位点严格递增。 */
    public synchronized AppendMessageResult putMessage(MessageExt msg) {
        long queueOffset = queueOffsets
                .computeIfAbsent(queueKey(msg.getTopic(), msg.getQueueId()), k -> new AtomicLong(0))
                .getAndIncrement();
        msg.setQueueOffset(queueOffset);
        msg.setStoreTimestamp(System.currentTimeMillis());
        if (msg.getDeliverTimestamp() == 0) {
            msg.setDeliverTimestamp(msg.getStoreTimestamp());
        }
        msg.getProperties().put(MessageSerializer.PROPERTY_TOPIC, msg.getTopic());

        // physicalOffset 在消息头里，必须先定位目标文件再编码
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
        msg.setPhysicalOffset(mappedFile.getFileFromOffset() + mappedFile.getWrotePosition());
        byte[] encoded = MessageSerializer.encode(msg);

        if (!mappedFile.appendMessage(encoded)) {
            // 当前文件剩余空间放不下本条，滚动后重编码（physicalOffset 变了）
            mappedFile = mappedFileQueue.rollNewFile();
            msg.setPhysicalOffset(mappedFile.getFileFromOffset());
            encoded = MessageSerializer.encode(msg);
            if (!mappedFile.appendMessage(encoded)) {
                throw new IllegalStateException("message larger than mapped file, size=" + encoded.length);
            }
        }
        return AppendMessageResult.ok(msg.getPhysicalOffset(), queueOffset, encoded.length);
    }

    public MessageExt getMessage(long physicalOffset) {
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(physicalOffset);
        if (mappedFile == null) {
            return null;
        }
        int pos = (int) (physicalOffset - mappedFile.getFileFromOffset());
        ByteBuffer header = mappedFile.select(pos, 4);
        int msgSize = header.getInt();
        if (msgSize < MessageSerializer.HEADER_LENGTH || pos + msgSize > mappedFile.getWrotePosition()) {
            return null;
        }
        try {
            return MessageSerializer.decode(mappedFile.select(pos, msgSize));
        } catch (MessageSerializer.CorruptMessageException e) {
            log.error("corrupt message at offset {}", physicalOffset, e);
            return null;
        }
    }

    /**
     * 崩溃恢复：从 recoverOffset 开始扫描，截断坏尾，重设 wrote/flushed 位点。
     *
     * @param recoverOffset 通常取 checkpoint.commitlogMaxFlushedOffset；<=0 表示从头扫
     * @return 恢复后的有效末尾位点
     */
    public long recover(long recoverOffset) {
        List<MappedFile> files = mappedFileQueue.getMappedFiles();
        if (files.isEmpty()) {
            return 0;
        }
        long scanOffset = Math.max(recoverOffset, 0);
        MappedFile startFile = mappedFileQueue.findMappedFileByOffset(scanOffset);
        int startIdx = startFile == null ? files.size() - 1 : files.indexOf(startFile);

        long validEnd = scanOffset;
        for (int i = startIdx; i < files.size(); i++) {
            MappedFile mf = files.get(i);
            int pos = (i == startIdx) ? (int) (scanOffset - mf.getFileFromOffset()) : 0;
            if (pos < 0 || pos >= mf.getFileSize()) {
                pos = 0; // checkpoint 越界（数据文件被清理过），保守重扫本文件
            }
            ByteBuffer buf = mf.selectToFileEnd(pos);
            int lastValid = pos;
            boolean truncated = false;
            try {
                while (true) {
                    int before = buf.position();
                    MessageExt msg = MessageSerializer.decode(buf);
                    if (msg == null) {
                        // 读不下去了：剩余区域全零说明文件是正常滚动结束，继续扫下一个文件；
                        // 含非零字节才是"写了半条"的坏尾，截断并丢弃后续文件
                        truncated = !isAllZero(buf);
                        lastValid = pos + before;
                        if (lastValid > 0 || pos == 0) {
                            validEnd = mf.getFileFromOffset() + lastValid;
                        }
                        break;
                    }
                    lastValid = pos + buf.position();
                    validEnd = mf.getFileFromOffset() + lastValid;
                }
            } catch (MessageSerializer.CorruptMessageException e) {
                log.warn("corrupt message in {} at filePos {}, truncate tail. detail: {}",
                        mf.getFileName(), pos + buf.position(), e.getMessage());
                truncated = true;
            }
            mf.setWrotePosition(lastValid);
            mf.setFlushedPosition(lastValid);
            if (truncated) {
                // 该文件之后的文件全部作废
                mappedFileQueue.removeFilesAfter(mf.getFileFromOffset() + lastValid);
                break;
            }
        }
        log.info("commitlog recovered, validEnd={}", validEnd);
        return validEnd;
    }

    /** 判断 buf 剩余区域是否全零（文件从未写入的区域），不移动读指针 */
    private static boolean isAllZero(ByteBuffer buf) {
        for (int i = buf.position(); i < buf.limit(); i++) {
            if (buf.get(i) != 0) {
                return false;
            }
        }
        return true;
    }

    /** 恢复时从 ConsumeQueue 的最大位点回填 queueOffsets，避免重启后位点回退 */
    public void setQueueOffset(String topic, int queueId, long nextOffset) {
        queueOffsets.put(queueKey(topic, queueId), new AtomicLong(nextOffset));
    }

    public long nextQueueOffset(String topic, int queueId) {
        AtomicLong v = queueOffsets.get(queueKey(topic, queueId));
        return v == null ? 0 : v.get();
    }

    public static String queueKey(String topic, int queueId) {
        return topic + "@" + queueId;
    }

    public long getMaxOffset() {
        return mappedFileQueue.getMaxOffset();
    }

    /** 顺序扫描结果：真实起始位点（跳过空洞后）+ 数据 */
    public static class DataSlice {
        public final long startOffset;
        public final ByteBuffer buffer;

        public DataSlice(long startOffset, ByteBuffer buffer) {
            this.startOffset = startOffset;
            this.buffer = buffer;
        }
    }

    /** 从 fromOffset 起读当前文件 [from, wrote) 的字节；落在文件尾部空洞时自动跳到下一个文件 */
    public DataSlice getData(long fromOffset) {
        long max = getMaxOffset();
        if (fromOffset >= max) {
            return null;
        }
        MappedFile mf = mappedFileQueue.findMappedFileByOffset(fromOffset);
        if (mf == null) {
            return null;
        }
        int pos = (int) (fromOffset - mf.getFileFromOffset());
        long realStart = fromOffset;
        if (pos >= mf.getWrotePosition()) {
            List<MappedFile> files = mappedFileQueue.getMappedFiles();
            int idx = files.indexOf(mf) + 1;
            if (idx >= files.size()) {
                return null;
            }
            mf = files.get(idx);
            pos = 0;
            realStart = mf.getFileFromOffset();
        }
        int len = mf.getWrotePosition() - pos;
        if (len <= 0) {
            return null;
        }
        return new DataSlice(realStart, mf.select(pos, len));
    }

    public void flush() {
        mappedFileQueue.flush();
    }

    public void shutdown() {
        mappedFileQueue.shutdown();
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }
}
