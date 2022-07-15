package bao.study.mymq.common.utils;

import com.alibaba.fastjson.JSON;

import java.nio.charset.StandardCharsets;

/**
 * @author baoyh
 * @since 2022/6/30 19:18
 */
public abstract class DataParseHelper {

    public static <T> T byte2Object(byte[] bytes, Class<T> clazz) {
        return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), clazz);
    }
}
