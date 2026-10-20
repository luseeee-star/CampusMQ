package com.campusmq.broker.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * MappedFile 序列管理：按文件名（起始偏移）排序，队尾为当前写入文件。
 * 文件滚动用 synchronized 保证只有一个线程创建新文件
 * （执行方案 §7 写的是 AtomicReference+CAS，这里先做最简单的正确性版本，实验期再对比）。
 */
public class MappedFileQueue {

    private static final Logger log = LoggerFactory.getLogger(MappedFileQueue.class);

    private final String storePath;
    private final int mappedFileSize;
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    public MappedFileQueue(String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
    }

    /** 启动时按文件名顺序加载已存在的文件 */
    public void load() {
        File dir = new File(storePath);
        if (!dir.exists()) {
            dir.mkdirs();
            return;
        }
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        Arrays.sort(files, (a, b) -> a.getName().compareTo(b.getName()));
        for (File file : files) {
            if (file.length() != mappedFileSize) {
                log.warn("file {} length {} not match mappedFileSize {}, ignore",
                        file.getName(), file.length(), mappedFileSize);
                continue;
            }
            MappedFile mf = new MappedFile(file.getPath(), mappedFileSize);
            try {
                mf.init();
                mappedFiles.add(mf);
            } catch (Exception e) {
                throw new IllegalStateException("load mapped file failed: " + file.getPath(), e);
            }
        }
        // 非队尾文件必然是写满的；队尾文件的有效位点由调用方（恢复/扫描）确定
        for (int i = 0; i < mappedFiles.size() - 1; i++) {
            mappedFiles.get(i).setWrotePosition(mappedFileSize);
            mappedFiles.get(i).setFlushedPosition(mappedFileSize);
        }
        log.info("load mapped files in {}, count={}", storePath, mappedFiles.size());
    }

    /** 取队尾文件；不存在或已满则滚动创建新文件。 */
    public synchronized MappedFile getLastMappedFile() {
        MappedFile last = mappedFiles.isEmpty() ? null : mappedFiles.get(mappedFiles.size() - 1);
        if (last == null || last.isFull()) {
            return rollNewFile();
        }
        return last;
    }

    /** 强制滚动创建新文件（当前文件剩余空间放不下本条消息时调用）。 */
    public synchronized MappedFile rollNewFile() {
        MappedFile last = mappedFiles.isEmpty() ? null : mappedFiles.get(mappedFiles.size() - 1);
        long nextOffset = last == null ? 0 : last.getFileFromOffset() + mappedFileSize;
        MappedFile created = new MappedFile(storePath + File.separator + formatOffset(nextOffset), mappedFileSize);
        try {
            created.init();
        } catch (Exception e) {
            throw new IllegalStateException("create mapped file failed", e);
        }
        mappedFiles.add(created);
        log.info("roll to new mapped file {}", created.getFileName());
        return created;
    }

    public static String formatOffset(long offset) {
        return String.format("%020d", offset);
    }

    /** 按全局物理偏移定位所在文件 */
    public MappedFile findMappedFileByOffset(long offset) {
        if (mappedFiles.isEmpty()) {
            return null;
        }
        int idx = (int) (offset / mappedFileSize);
        if (idx < mappedFiles.size()) {
            MappedFile mf = mappedFiles.get(idx);
            if (mf.getFileFromOffset() <= offset) {
                return mf;
            }
        }
        // 兜底线性找（文件被截断移除过时会走到这里）
        for (MappedFile mf : mappedFiles) {
            if (offset >= mf.getFileFromOffset() && offset < mf.getFileFromOffset() + mappedFileSize) {
                return mf;
            }
        }
        return null;
    }

    /** 全局最大有效位点：队尾文件 fromOffset + wrotePosition */
    public long getMaxOffset() {
        if (mappedFiles.isEmpty()) {
            return 0;
        }
        MappedFile last = mappedFiles.get(mappedFiles.size() - 1);
        return last.getFileFromOffset() + last.getWrotePosition();
    }

    /** 截断位点之后的文件从内存中摘除（磁盘文件保留，供事后排查） */
    public void removeFilesAfter(long offset) {
        List<MappedFile> toRemove = new ArrayList<>();
        for (MappedFile mf : mappedFiles) {
            if (mf.getFileFromOffset() > offset) {
                toRemove.add(mf);
            }
        }
        toRemove.forEach(mf -> {
            mf.shutdown();
            mappedFiles.remove(mf);
        });
    }

    public void flush() {
        for (MappedFile mf : mappedFiles) {
            mf.flush();
        }
    }

    public void shutdown() {
        for (MappedFile mf : mappedFiles) {
            mf.shutdown();
        }
        mappedFiles.clear();
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }
}
