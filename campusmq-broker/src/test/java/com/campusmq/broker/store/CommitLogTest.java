package com.campusmq.broker.store;

import com.campusmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * D2/D3 验收：文件滚动（最高危项）+ 编码解码 + 重启恢复 + 坏尾截断。
 * 用小尺寸映射文件（4KB）强制触发滚动。
 */
class CommitLogTest {

    private static final int SMALL_FILE = 4 * 1024;

    private MessageExt msg(String topic, String body) {
        MessageExt m = new MessageExt();
        m.setTopic(topic);
        m.setQueueId(0);
        m.setBody(body.getBytes());
        m.setProperties(new HashMap<>(Map.of("tags", "test")));
        return m;
    }

    private String storeDir(String name) {
        String dir = "target/test-store/" + name;
        TestStoreCleaner.clean(dir);
        return dir;
    }

    @Test
    void appendAndRollAcrossFiles() {
        String dir = storeDir("commitlog-roll");
        CommitLog commitLog = new CommitLog(dir, SMALL_FILE);
        commitLog.load();

        // 每条消息约 100B，4KB 文件只能装约 40 条，300 条必然跨多个文件
        int total = 300;
        long[] physicalOffsets = new long[total];
        for (int i = 0; i < total; i++) {
            AppendMessageResult r = commitLog.putMessage(msg("topicA", "msg-" + i));
            assertEquals(AppendMessageResult.Status.OK, r.getStatus());
            assertEquals(i, r.getQueueOffset());
            physicalOffsets[i] = r.getPhysicalOffset();
        }
        assertTrue(commitLog.getMappedFileQueue().getMappedFiles().size() > 1,
                "should roll to multiple files");

        // 逐条读回校验：跨文件读取正确（文件边界有空洞，必须用写入时返回的物理偏移）
        for (int i = 0; i < total; i++) {
            MessageExt got = commitLog.getMessage(physicalOffsets[i]);
            assertNotNull(got, "message at offset " + physicalOffsets[i]);
            assertEquals("msg-" + i, got.bodyString());
            assertEquals(i, got.getQueueOffset());
            assertEquals(physicalOffsets[i], got.getPhysicalOffset());
        }
        commitLog.shutdown();
    }

    private int encodedLen(MessageExt m) {
        return com.campusmq.common.message.MessageSerializer.encode(m).length;
    }

    @Test
    void recoverAfterRestartKeepsAllMessages() {
        String dir = storeDir("commitlog-recover");
        CommitLog first = new CommitLog(dir, SMALL_FILE);
        first.load();
        int total = 100;
        for (int i = 0; i < total; i++) {
            first.putMessage(msg("topicB", "persist-" + i));
        }
        first.flush();
        first.shutdown();

        // 重启（无 checkpoint，从头扫描恢复）
        CommitLog second = new CommitLog(dir, SMALL_FILE);
        second.load();
        long validEnd = second.recover(0);
        assertEquals(second.getMaxOffset(), validEnd);

        // 全部消息仍可读（按写入返回的物理偏移逐条读）
        int count = 0;
        for (long off = 0; off < validEnd && count < total; count++) {
            CommitLog.DataSlice slice = second.getData(off);
            assertNotNull(slice, "data at " + off);
            com.campusmq.common.message.MessageExt m =
                    com.campusmq.common.message.MessageSerializer.decode(slice.buffer);
            assertNotNull(m);
            assertEquals("persist-" + count, m.bodyString());
            off = slice.startOffset + encodedLen(m);
        }
        assertEquals(total, count);
        second.shutdown();
    }

    @Test
    void corruptTailIsTruncatedOnRecover() throws Exception {
        String dir = storeDir("commitlog-corrupt");
        CommitLog first = new CommitLog(dir, SMALL_FILE);
        first.load();
        int total = 50;
        long lastOffset = 0;
        int lastSize = 0;
        for (int i = 0; i < total; i++) {
            AppendMessageResult r = first.putMessage(msg("topicC", "safe-" + i));
            lastOffset = r.getPhysicalOffset();
            lastSize = r.getWroteBytes();
        }
        first.flush();
        first.shutdown();

        // 模拟"写一半被 kill"：把最后一条消息的后半段改坏
        File commitlogDir = new File(dir + "/commitlog");
        File lastFile = null;
        for (File f : commitlogDir.listFiles()) {
            if (lastFile == null || f.getName().compareTo(lastFile.getName()) > 0) {
                lastFile = f;
            }
        }
        long fileFrom = Long.parseLong(lastFile.getName());
        int posInFile = (int) (lastOffset - fileFrom);
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(lastFile, "rw")) {
            raf.seek(posInFile + lastSize - 3); // 破坏消息体尾部，CRC 必然不匹配
            raf.write(new byte[]{0x11, 0x22, 0x33});
        }

        CommitLog second = new CommitLog(dir, SMALL_FILE);
        second.load();
        long validEnd = second.recover(0);
        // 坏尾被丢弃，有效末尾 = 最后一条消息的起点
        assertEquals(lastOffset, validEnd);

        int count = 0;
        for (long off = 0; off < validEnd && count < total; count++) {
            CommitLog.DataSlice slice = second.getData(off);
            assertNotNull(slice, "data at " + off);
            MessageExt m = com.campusmq.common.message.MessageSerializer.decode(slice.buffer);
            assertNotNull(m);
            assertEquals("safe-" + count, m.bodyString());
            off = slice.startOffset + encodedLen(m);
        }
        assertEquals(total - 1, count);
        second.shutdown();
    }
}
