package com.campusmq.broker.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 稀疏索引（执行方案 §3.3）：20 字节定长条目——
 * <pre>
 * commitlogOffset 8B  指向 CommitLog 的物理偏移
 * msgSize         4B  该条消息总长度
 * tagsHash        8B  标签哈希；延迟消息复用存投递时刻
 * </pre>
 * 定长是为了二分查找；20 字节让一个 4K 页恰好装 204 条。
 */
public class ConsumeQueue {

    private static final Logger log = LoggerFactory.getLogger(ConsumeQueue.class);

    public static final int CQ_ITEM_SIZE = 20;

    private final String topic;
    private final int queueId;
    private final MappedFileQueue mappedFileQueue;

    public ConsumeQueue(String topic, int queueId, String storePath, int mappedFileSize) {
        this.topic = topic;
        this.queueId = queueId;
        // mappedFileSize 必须是 20 的倍数
        int aligned = Math.max(CQ_ITEM_SIZE, mappedFileSize / CQ_ITEM_SIZE * CQ_ITEM_SIZE);
        String dir = storePath + java.io.File.separator + "consumequeue"
                + java.io.File.separator + topic + java.io.File.separator + queueId;
        this.mappedFileQueue = new MappedFileQueue(dir, aligned);
    }

    public void load() {
        mappedFileQueue.load();
        // 队尾文件的有效位点：顺序扫描 20B 槽位，遇到全零槽即结束（size=0 不可能是合法条目）
        if (!mappedFileQueue.getMappedFiles().isEmpty()) {
            MappedFile last = mappedFileQueue.getMappedFiles()
                    .get(mappedFileQueue.getMappedFiles().size() - 1);
            int pos = 0;
            while (pos + CQ_ITEM_SIZE <= last.getFileSize()) {
                ByteBuffer buf = last.select(pos, CQ_ITEM_SIZE);
                boolean allZero = true;
                while (buf.hasRemaining()) {
                    if (buf.get() != 0) {
                        allZero = false;
                        break;
                    }
                }
                if (allZero) {
                    break;
                }
                pos += CQ_ITEM_SIZE;
            }
            last.setWrotePosition(pos);
            last.setFlushedPosition(pos);
        }
    }

    /** 追加一条索引。返回当前队列最大逻辑位点（= 本条 queueOffset + 1）。 */
    public synchronized long putMessagePositionInfo(long commitlogOffset, int msgSize, long tagsHash) {
        ByteBuffer item = ByteBuffer.allocate(CQ_ITEM_SIZE);
        item.putLong(commitlogOffset);
        item.putInt(msgSize);
        item.putLong(tagsHash);
        MappedFile mf = mappedFileQueue.getLastMappedFile();
        if (!mf.appendMessage(item.array())) {
            mf = mappedFileQueue.rollNewFile();
            if (!mf.appendMessage(item.array())) {
                throw new IllegalStateException("append consumequeue index failed");
            }
        }
        return getMaxOffsetInQueue();
    }

    /** 读逻辑位点 logicOffset 处的 20 字节索引项；越界返回 null。 */
    public ByteBuffer getIndexBuffer(long logicOffset) {
        long byteOffset = logicOffset * CQ_ITEM_SIZE;
        MappedFile mf = mappedFileQueue.findMappedFileByOffset(byteOffset);
        if (mf == null) {
            return null;
        }
        int pos = (int) (byteOffset - mf.getFileFromOffset());
        if (pos + CQ_ITEM_SIZE > mf.getWrotePosition()) {
            return null;
        }
        return mf.select(pos, CQ_ITEM_SIZE);
    }

    public long getMaxOffsetInQueue() {
        return mappedFileQueue.getMaxOffset() / CQ_ITEM_SIZE;
    }

    public void flush() {
        mappedFileQueue.flush();
    }

    public void shutdown() {
        mappedFileQueue.shutdown();
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }
}
