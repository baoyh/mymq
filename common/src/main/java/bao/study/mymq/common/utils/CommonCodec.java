package bao.study.mymq.common.utils;

import com.alibaba.fastjson.JSON;

import java.nio.charset.StandardCharsets;

/**
 * @author baoyh
 * @since 2022/6/30 19:18
 */
public abstract class CommonCodec {

    public static <T> T decode(byte[] bytes, Class<T> clazz) {
        return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), clazz);
    }

    public static byte[] encode(Object obj) {
        return JSON.toJSONString(obj).getBytes(StandardCharsets.UTF_8);
    }
}
