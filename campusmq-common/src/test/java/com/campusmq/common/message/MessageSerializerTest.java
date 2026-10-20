package com.campusmq.common.message;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageSerializerTest {

    @Test
    void encodeThenDecodeRoundTrip() {
        MessageExt msg = new MessageExt();
        msg.setTopic("quota_claimed");
        msg.setQueueId(3);
        msg.setQueueOffset(1024L);
        msg.setPhysicalOffset(4096L);
        msg.setStoreTimestamp(System.currentTimeMillis());
        msg.setDeliverTimestamp(msg.getStoreTimestamp());
        msg.setReconsumeTimes(2);
        msg.setProperties(Map.of("topic", "quota_claimed", "tags", "lecture"));
        msg.setBody("hello campusmq".getBytes());

        byte[] encoded = MessageSerializer.encode(msg);
        assertEquals(MessageSerializer.HEADER_LENGTH + msg.propertiesString().length() + msg.getBody().length,
                encoded.length);

        ByteBuffer buf = ByteBuffer.wrap(encoded);
        MessageExt decoded = MessageSerializer.decode(buf);
        assertNotNull(decoded);
        assertEquals(encoded.length, buf.position());
        assertEquals(3, decoded.getQueueId());
        assertEquals(1024L, decoded.getQueueOffset());
        assertEquals(4096L, decoded.getPhysicalOffset());
        assertEquals(2, decoded.getReconsumeTimes());
        assertEquals("quota_claimed", decoded.getTopic());
        assertEquals("lecture", decoded.getProperties().get("tags"));
        assertArrayEquals(msg.getBody(), decoded.getBody());
    }

    @Test
    void decodeMultipleMessagesSequentially() {
        MessageExt a = new MessageExt();
        a.setProperties(Map.of("topic", "t"));
        a.setBody("a".getBytes());
        MessageExt b = new MessageExt();
        b.setProperties(Map.of("topic", "t"));
        b.setBody("b".getBytes());

        byte[] ea = MessageSerializer.encode(a);
        byte[] eb = MessageSerializer.encode(b);
        ByteBuffer buf = ByteBuffer.allocate(ea.length + eb.length);
        buf.put(ea).put(eb).flip();

        MessageExt da = MessageSerializer.decode(buf);
        MessageExt db = MessageSerializer.decode(buf);
        assertEquals("a", da.bodyString());
        assertEquals("b", db.bodyString());
        assertNull(MessageSerializer.decode(buf));
    }

    @Test
    void corruptMagicIsDetected() {
        MessageExt a = new MessageExt();
        a.setProperties(Map.of("topic", "t"));
        a.setBody("x".getBytes());
        byte[] encoded = MessageSerializer.encode(a);
        // 破坏 magicCode
        encoded[4] = 0x11;
        encoded[5] = 0x22;

        assertThrows(MessageSerializer.CorruptMessageException.class,
                () -> MessageSerializer.decode(ByteBuffer.wrap(encoded)));
    }

    @Test
    void corruptBodyCrcIsDetected() {
        MessageExt a = new MessageExt();
        a.setProperties(Map.of("topic", "t"));
        a.setBody("original".getBytes());
        byte[] encoded = MessageSerializer.encode(a);
        // 破坏消息体最后一个字节
        encoded[encoded.length - 1] ^= 0xFF;

        assertThrows(MessageSerializer.CorruptMessageException.class,
                () -> MessageSerializer.decode(ByteBuffer.wrap(encoded)));
    }

    @Test
    void incompleteTailReturnsNull() {
        MessageExt a = new MessageExt();
        a.setProperties(Map.of("topic", "t"));
        a.setBody("some body".getBytes());
        byte[] encoded = MessageSerializer.encode(a);
        // 截掉尾部 3 字节模拟写了一半
        ByteBuffer truncated = ByteBuffer.wrap(encoded, 0, encoded.length - 3);
        assertNull(MessageSerializer.decode(truncated));
    }
}
