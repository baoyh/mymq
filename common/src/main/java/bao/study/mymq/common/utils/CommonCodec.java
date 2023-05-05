package bao.study.mymq.common.utils;


import com.google.gson.Gson;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author baoyh
 * @since 2022/6/30 19:18
 */
public abstract class CommonCodec {

    private static final Gson gson = new Gson();

    private static final Charset charset = StandardCharsets.UTF_8;

    public static <T> T decode(byte[] bytes, Class<T> clazz) {
        return gson.fromJson(new String(bytes, charset), clazz);
    }

    public static <T> List<T> decodeAsList(byte[] bytes, Class<T> clazz) {
        return gson.fromJson(new String(bytes, charset), new ParameterizedTypeImpl(clazz));
    }

    public static <T> T decode(String json, Class<T> clazz) {
        return gson.fromJson(json, clazz);
    }

    public static byte[] encode(Object obj) {
        return gson.toJson(obj).getBytes(charset);
    }

    public static String encode2String(Object obj) {
        return gson.toJson(obj);
    }

    private static class ParameterizedTypeImpl implements ParameterizedType {

        Class<?> clazz;

        public ParameterizedTypeImpl(Class<?> clz) {
            clazz = clz;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return new Type[]{clazz};
        }

        @Override
        public Type getRawType() {
            return List.class;
        }

        @Override
        public Type getOwnerType() {
            return null;
        }
    }
}
