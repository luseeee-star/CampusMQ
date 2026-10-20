package com.campusmq.common.netty;

import com.campusmq.common.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/** 16 字节头 + body -> RemotingCommand。半包时重置读指针等待下次。 */
public class NettyDecoder extends ByteToMessageDecoder {

    /** 单帧上限 8MB，防脏数据撑爆内存 */
    private static final int MAX_FRAME = 8 * 1024 * 1024;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 4) {
            return;
        }
        in.markReaderIndex();
        int length = in.readInt();
        if (length < RemotingCommand.HEADER_LENGTH || length > MAX_FRAME) {
            ctx.close();
            throw new IllegalStateException("illegal frame length: " + length);
        }
        if (in.readableBytes() < length - 4) {
            in.resetReaderIndex();
            return;
        }

        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(in.readShort());
        in.readByte();          // version
        in.readByte();          // serializeType
        cmd.setOpaque(in.readInt());
        cmd.setFlag(in.readInt());

        int bodyLen = length - RemotingCommand.HEADER_LENGTH;
        if (bodyLen > 0) {
            byte[] body = new byte[bodyLen];
            in.readBytes(body);
            cmd.setBody(body);
        }
        out.add(cmd);
    }
}
