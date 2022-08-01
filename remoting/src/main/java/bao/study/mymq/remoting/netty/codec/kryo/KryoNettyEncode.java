package bao.study.mymq.remoting.netty.codec.kryo;

import bao.study.mymq.remoting.common.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author baoyh
 * @since 2022/5/26 13:55
 */
public class KryoNettyEncode extends MessageToByteEncoder<RemotingCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand msg, ByteBuf out) {
        KryoCodec.encode(msg, out);
        ctx.flush();
    }
}
