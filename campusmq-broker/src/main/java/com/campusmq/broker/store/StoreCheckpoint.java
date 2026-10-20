package com.campusmq.broker.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 检查点文件（执行方案 §3.4），每条 8 字节：
 * commitlogMaxFlushedOffset / consumeQueueMaxBuiltOffset / delayLogMaxValidOffset（预留）。
 */
public class StoreCheckpoint {

    private static final Logger log = LoggerFactory.getLogger(StoreCheckpoint.class);

    private final String filePath;
    private volatile long commitlogMaxFlushedOffset;
    private volatile long consumeQueueMaxBuiltOffset;
    private volatile long delayLogMaxValidOffset;

    public StoreCheckpoint(String storePath) {
        this.filePath = storePath + File.separator + "checkpoint";
    }

    public void load() {
        File file = new File(filePath);
        if (!file.exists()) {
            log.info("checkpoint file not exists, start from 0");
            return;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
            this.commitlogMaxFlushedOffset = in.readLong();
            this.consumeQueueMaxBuiltOffset = in.readLong();
            this.delayLogMaxValidOffset = in.readLong();
            log.info("checkpoint loaded: flushed={}, reput={}", commitlogMaxFlushedOffset, consumeQueueMaxBuiltOffset);
        } catch (IOException e) {
            log.warn("load checkpoint failed, start from 0", e);
        }
    }

    public void save() {
        try {
            File file = new File(filePath);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
                out.writeLong(commitlogMaxFlushedOffset);
                out.writeLong(consumeQueueMaxBuiltOffset);
                out.writeLong(delayLogMaxValidOffset);
            }
        } catch (IOException e) {
            log.error("save checkpoint failed", e);
        }
    }

    public long getCommitlogMaxFlushedOffset() {
        return commitlogMaxFlushedOffset;
    }

    public void setCommitlogMaxFlushedOffset(long v) {
        this.commitlogMaxFlushedOffset = v;
    }

    public long getConsumeQueueMaxBuiltOffset() {
        return consumeQueueMaxBuiltOffset;
    }

    public void setConsumeQueueMaxBuiltOffset(long v) {
        this.consumeQueueMaxBuiltOffset = v;
    }

    public long getDelayLogMaxValidOffset() {
        return delayLogMaxValidOffset;
    }
}
