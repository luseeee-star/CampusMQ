package com.campusmq.broker.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个内存映射文件。文件名即起始物理偏移（20 位数字）。
 *
 * 为什么用一把写锁（执行方案 §3.1）：MappedByteBuffer.put() 不是原子的，
 * 多线程并发 append 会导致消息体交叉写坏。ReentrantLock 是最简单的正确性保证，
 * 也是实验 E2 调优时第一个该质疑的点。
 */
public class MappedFile {

    private static final Logger log = LoggerFactory.getLogger(MappedFile.class);

    private final String fileName;
    private final long fileFromOffset;
    private final int fileSize;
    private final File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedBuffer;

    private final AtomicInteger wrotePosition = new AtomicInteger(0);
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    private final Lock writeLock = new ReentrantLock();

    public MappedFile(String fileName, int fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
    }

    public void init() throws Exception {
        ensureDir();
        this.randomAccessFile = new RandomAccessFile(this.file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
    }

    private void ensureDir() {
        File parent = this.file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
    }

    /** 追加写入；空间不足返回 false（调用方负责滚动到新文件）。 */
    public boolean appendMessage(byte[] data) {
        writeLock.lock();
        try {
            int wrote = wrotePosition.get();
            if (wrote + data.length > fileSize) {
                return false;
            }
            ByteBuffer slice = mappedBuffer.slice();
            slice.position(wrote);
            slice.put(data);
            wrotePosition.addAndGet(data.length);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /** 刷盘：把 [flushedPosition, wrotePosition) 落到磁盘。返回已刷位点。 */
    public int flush() {
        int wrote = wrotePosition.get();
        if (flushedPosition.get() < wrote) {
            mappedBuffer.force();
            flushedPosition.set(wrote);
        }
        return flushedPosition.get();
    }

    /** 读 [pos, pos+len)，基于已写入区域的只读视图 */
    public ByteBuffer select(int pos, int len) {
        ByteBuffer view = mappedBuffer.slice();
        view.position(pos);
        view.limit(pos + len);
        return view.slice();
    }

    /** 读 [pos, wrotePosition) */
    public ByteBuffer selectWrote(int pos) {
        return select(pos, wrotePosition.get() - pos);
    }

    /** 读 [pos, fileSize)，恢复扫描用（尾部未写区域全零，msgSize=0 会被识别为结束） */
    public ByteBuffer selectToFileEnd(int pos) {
        return select(pos, fileSize - pos);
    }

    public boolean isFull() {
        return wrotePosition.get() >= fileSize;
    }

    public void shutdown() {
        flush();
        try {
            unmap();
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (Exception e) {
            log.warn("close mapped file {} failed", fileName, e);
        }
    }

    /** Windows 下显式解除映射，否则文件句柄被 JVM 持有无法删除 */
    private void unmap() {
        try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
            Method invokeCleaner = sun.misc.Unsafe.class.getMethod("invokeCleaner", java.nio.ByteBuffer.class);
            invokeCleaner.invoke(unsafe, mappedBuffer);
        } catch (Throwable t) {
            log.debug("unmap failed, rely on GC: {}", t.getMessage());
        }
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getFileSize() {
        return fileSize;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }
}
