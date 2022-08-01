package bao.study.mymq.remoting.netty.codec.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author baoyh
 * @since 2022/7/27 14:17
 */
public abstract class KryoCodec {

    private static final ThreadLocalKryoFactory factory = new ThreadLocalKryoFactory();

    public static void encode(Object object, ByteBuf out) {
        Kryo kryo = factory.getKryo();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, object);
        output.flush();
        output.close();
        byte[] b = baos.toByteArray();
        try {
            baos.flush();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        out.writeInt(b.length);
        out.writeBytes(b);
    }

    public static Object decode(ByteBuf in) {

        if (in == null) {
            return null;
        }

        if (in.readableBytes() >= 4) {
            in.markReaderIndex();
            int dataLength = in.readInt();

            if (dataLength < 0 || in.readableBytes() < 0) {
                return null;
            }

            if (in.readableBytes() < dataLength) {
                in.resetReaderIndex();
                return null;
            }

            byte[] body = new byte[dataLength];
            in.readBytes(body);
            Input input = new Input(new ByteArrayInputStream(body));
            Kryo kryo = factory.getKryo();
            return kryo.readClassAndObject(input);
        }

        return null;
    }
}
