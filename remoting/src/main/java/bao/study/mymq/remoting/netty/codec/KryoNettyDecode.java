package bao.study.mymq.remoting.netty.codec;

import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.protocol.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
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
        Kryo kryo = KryoFactory.getKryo();
        kryo.register(RemotingCommand.class);

        Input input = new Input(new ByteBufInputStream(in));
        out.add(kryo.readClassAndObject(input));
    }
}
