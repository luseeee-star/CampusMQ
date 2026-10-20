package com.campusmq.common.netty;

import com.campusmq.common.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/** RemotingCommand -> 16 字节头 + body。 */
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand cmd, ByteBuf out) {
        byte[] body = cmd.getBody();
        int bodyLen = body == null ? 0 : body.length;
        int totalLen = RemotingCommand.HEADER_LENGTH + bodyLen;

        out.writeInt(totalLen);
        out.writeShort(cmd.getCode());
        out.writeByte(cmd.getVersion());
        out.writeByte(cmd.getSerializeType());
        out.writeInt(cmd.getOpaque());
        out.writeInt(cmd.getFlag());
        if (bodyLen > 0) {
            out.writeBytes(body);
        }
    }
}
