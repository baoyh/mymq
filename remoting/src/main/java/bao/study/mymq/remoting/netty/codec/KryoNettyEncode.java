package bao.study.mymq.remoting.netty.codec;

import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.protocol.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author baoyh
 * @since 2022/5/26 13:55
 */
public class KryoNettyEncode extends MessageToByteEncoder<RemotingCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand msg, ByteBuf out) {
        Kryo kryo = KryoFactory.getKryo();
        kryo.register(RemotingCommand.class);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        kryo.writeClassAndObject(output, msg);
        output.flush();
        output.close();
        byte[] b = outputStream.toByteArray();
        try {
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        out.writeBytes(b);
        ctx.flush();
    }
}
