package com.campusmq.broker.store;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/** D4 验收：20B 定长索引写入 / 读取 / 滚动。 */
class ConsumeQueueTest {

    @Test
    void putAndGetIndexAcrossFiles() {
        TestStoreCleaner.clean("target/test-store/cq");
        // 小文件强制滚动：200B = 10 条/文件
        ConsumeQueue cq = new ConsumeQueue("quota_claimed", 0, "target/test-store/cq", 200);
        cq.load();

        int total = 55;
        for (int i = 0; i < total; i++) {
            long max = cq.putMessagePositionInfo(i * 100L, 100, i * 1000L);
            assertEquals(i + 1, max);
        }
        assertEquals(total, cq.getMaxOffsetInQueue());

        for (int i = 0; i < total; i++) {
            ByteBuffer index = cq.getIndexBuffer(i);
            assertNotNull(index, "index at " + i);
            assertEquals(i * 100L, index.getLong());
            assertEquals(100, index.getInt());
            assertEquals(i * 1000L, index.getLong());
        }
        assertNull(cq.getIndexBuffer(total));
        cq.shutdown();
    }

    @Test
    void reloadKeepsMaxOffset() {
        String dir = "target/test-store/cq-reload";
        TestStoreCleaner.clean(dir);
        ConsumeQueue first = new ConsumeQueue("t", 0, dir, 200);
        first.load();
        for (int i = 0; i < 30; i++) {
            first.putMessagePositionInfo(i * 50L, 50, 0);
        }
        first.flush();
        first.shutdown();

        ConsumeQueue second = new ConsumeQueue("t", 0, dir, 200);
        second.load();
        // 重启后逻辑位点续得上
        assertEquals(30, second.getMaxOffsetInQueue());
        ByteBuffer index = second.getIndexBuffer(29);
        assertEquals(29 * 50L, index.getLong());
        second.shutdown();
    }
}
