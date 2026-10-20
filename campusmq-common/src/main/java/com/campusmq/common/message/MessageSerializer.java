package com.campusmq.common.message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * CommitLog 单条消息二进制格式（执行方案 §3.2），定长头 62 字节：
 * <pre>
 * msgSize           4B  本条总长度，含自身——崩溃扫描靠它跳到下一条
 * magicCode         4B  魔数——读到非预期值说明从这里开始损坏，直接截断
 * bodyCRC           4B  消息体 CRC32
 * queueId           4B
 * queueOffset       8B  队列内逻辑位点
 * physicalOffset    8B  commitlog 物理偏移
 * flag              4B  bit0 是否延迟消息
 * storeTimestamp    8B
 * deliverTimestamp  8B  内嵌头部，恢复延迟消息不用解析 properties
 * reconsumeTimes    4B
 * propertiesLength  2B
 * properties        NB  竖线分隔键值对；topic 也放在这里
 * bodyLength        4B
 * body              NB
 * </pre>
 */
public final class MessageSerializer {

    public static final int MAGIC_CODE = 0xC0FFEE01;
    /** msgSize4 + magic4 + crc4 + queueId4 + queueOffset8 + physicalOffset8 + flag4
     *  + storeTs8 + deliverTs8 + reconsume4 + propsLen2 + bodyLen4 = 62 */
    public static final int HEADER_LENGTH = 62;

    public static final String PROPERTY_TOPIC = "topic";

    private MessageSerializer() {
    }

    public static byte[] encode(MessageExt msg) {
        byte[] props = msg.propertiesString().getBytes(StandardCharsets.UTF_8);
        byte[] body = msg.getBody() == null ? new byte[0] : msg.getBody();
        if (props.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("properties too long: " + props.length);
        }
        int total = HEADER_LENGTH + props.length + body.length;

        ByteBuffer buf = ByteBuffer.allocate(total);
        buf.putInt(total);
        buf.putInt(MAGIC_CODE);
        buf.putInt(crc32(body));
        buf.putInt(msg.getQueueId());
        buf.putLong(msg.getQueueOffset());
        buf.putLong(msg.getPhysicalOffset());
        buf.putInt(msg.getFlag());
        buf.putLong(msg.getStoreTimestamp());
        buf.putLong(msg.getDeliverTimestamp());
        buf.putInt(msg.getReconsumeTimes());
        buf.putShort((short) props.length);
        buf.put(props);
        buf.putInt(body.length);
        buf.put(body);
        return buf.array();
    }

    /**
     * 从 buf 当前位置解码一条消息。
     *
     * @return 消息；null 表示当前位置不足一条完整消息（读到尾部）
     * @throws CorruptMessageException magic / CRC 校验失败，调用方应截断
     */
    public static MessageExt decode(ByteBuffer buf) {
        if (buf.remaining() < HEADER_LENGTH) {
            return null;
        }
        int start = buf.position();
        int msgSize = buf.getInt();
        if (msgSize < HEADER_LENGTH || buf.remaining() < msgSize - 4) {
            buf.position(start);
            return null;
        }
        int magic = buf.getInt();
        if (magic != MAGIC_CODE) {
            buf.position(start);
            throw new CorruptMessageException("bad magic at pos " + start);
        }
        int bodyCRC = buf.getInt();
        MessageExt msg = new MessageExt();
        msg.setQueueId(buf.getInt());
        msg.setQueueOffset(buf.getLong());
        msg.setPhysicalOffset(buf.getLong());
        msg.setFlag(buf.getInt());
        msg.setStoreTimestamp(buf.getLong());
        msg.setDeliverTimestamp(buf.getLong());
        msg.setReconsumeTimes(buf.getInt());

        int propsLen = buf.getShort() & 0xFFFF;
        byte[] props = new byte[propsLen];
        buf.get(props);
        msg.setProperties(MessageExt.stringToProperties(new String(props, StandardCharsets.UTF_8)));
        msg.setTopic(msg.getProperties().get(PROPERTY_TOPIC));

        int bodyLen = buf.getInt();
        byte[] body = new byte[bodyLen];
        buf.get(body);
        if (crc32(body) != bodyCRC) {
            buf.position(start);
            throw new CorruptMessageException("bad crc at pos " + start);
        }
        msg.setBody(body);
        return msg;
    }

    /** 只看头 4 字节拿 msgSize，用于恢复扫描时快速跳跃 */
    public static int peekMsgSize(ByteBuffer buf, int pos) {
        if (buf.limit() - pos < 4) {
            return -1;
        }
        return buf.getInt(pos);
    }

    private static int crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    public static class CorruptMessageException extends RuntimeException {
        public CorruptMessageException(String msg) {
            super(msg);
        }
    }
}
