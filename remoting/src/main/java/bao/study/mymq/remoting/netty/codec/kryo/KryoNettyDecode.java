package bao.study.mymq.remoting.netty.codec.kryo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * @author baoyh
 * @since 2022/5/26 14:03
 */
public class KryoNettyDecode extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        Object obj = KryoCodec.decode(in);
        if (obj != null) out.add(obj);
    }
}
