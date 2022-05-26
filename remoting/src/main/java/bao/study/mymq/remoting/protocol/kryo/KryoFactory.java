package bao.study.mymq.remoting.protocol.kryo;

import com.esotericsoftware.kryo.Kryo;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2022/5/26 11:33
 */
public abstract class KryoFactory {

    private static final ThreadLocal<Kryo> kryoContent = ThreadLocal.withInitial(KryoFactory::createKryo);

    private static Kryo createKryo() {
        Kryo kryo = new Kryo();
        kryo.register(Byte.class);
        kryo.register(Byte[].class);
        kryo.register(String.class);
        kryo.register(StringBuilder.class);
        kryo.register(StringBuffer.class);
        kryo.register(Integer.class);
        kryo.register(Long.class);
        kryo.register(Short.class);
        kryo.register(Float.class);
        kryo.register(Double.class);
        kryo.register(Boolean.class);
        kryo.register(byte[].class);
        kryo.register(char[].class);
        kryo.register(int[].class);
        kryo.register(float[].class);
        kryo.register(double[].class);
        kryo.register(HashMap.class);
        kryo.register(ConcurrentHashMap.class);
        kryo.register(LocalDateTime.class);
        return kryo;
    }

    public static Kryo getKryo() {
        return kryoContent.get();
    }
}
