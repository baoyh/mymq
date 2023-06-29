package bao.study.mymq.broker.util;

import bao.study.mymq.common.Constant;

/**
 * @author baoyh
 * @since 2023/6/28 17:08
 */
public abstract class MessageStoreHelper {

    public static String createKey(String topic, int queueId) {
        return topic + Constant.TOPIC_SEPARATOR + queueId;
    }

    public static String topic(String key) {
        return key.split(Constant.TOPIC_SEPARATOR)[0];
    }

    public static int queueId(String key) {
        return Integer.parseInt(key.split(Constant.TOPIC_SEPARATOR)[1]);
    }
}
