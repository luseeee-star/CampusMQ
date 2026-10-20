package com.campusmq.common.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 协议帧。线上格式（执行方案 §4.1，固定 16 字节头 + 变长 body）：
 * <pre>
 * length      4B  整包长度，含自身
 * code        2B  请求码
 * version     1B  协议版本
 * serialize   1B  序列化方式：0=JSON
 * opaque      4B  请求序号，异步配对响应用
 * flag        4B  扩展标志；bit0=1 表示这是响应
 * body        NB  JSON 序列化的业务对象
 * </pre>
 */
public class RemotingCommand {

    public static final int HEADER_LENGTH = 16;
    public static final byte SERIALIZE_JSON = 0;
    public static final int FLAG_RESPONSE = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private int code;
    private byte version = 1;
    private byte serializeType = SERIALIZE_JSON;
    private int opaque;
    private int flag;
    private byte[] body;

    public static RemotingCommand request(int code, int opaque, Object bodyObj) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.code = code;
        cmd.opaque = opaque;
        cmd.setBodyObj(bodyObj);
        return cmd;
    }

    public static RemotingCommand response(RemotingCommand req, Object bodyObj) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.code = req.code;
        cmd.opaque = req.opaque;
        cmd.flag = FLAG_RESPONSE;
        cmd.setBodyObj(bodyObj);
        return cmd;
    }

    public boolean isResponse() {
        return (flag & FLAG_RESPONSE) != 0;
    }

    public void setBodyObj(Object obj) {
        if (obj == null) {
            this.body = null;
            return;
        }
        try {
            this.body = MAPPER.writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new IllegalStateException("serialize body failed", e);
        }
    }

    public <T> T bodyAs(Class<T> type) {
        if (body == null) {
            return null;
        }
        try {
            return MAPPER.readValue(body, type);
        } catch (Exception e) {
            throw new IllegalStateException("deserialize body failed", e);
        }
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public byte getVersion() {
        return version;
    }

    public byte getSerializeType() {
        return serializeType;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
