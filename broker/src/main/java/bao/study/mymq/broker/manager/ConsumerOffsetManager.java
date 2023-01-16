package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.util.BrokerConfigHelper;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.utils.CommonCodec;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author baoyh
 * @since 2022/10/25 17:15
 */
public class ConsumerOffsetManager extends ConfigManager {

    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer/* queueId */, Long/* offset */>> consumedOffset = new ConcurrentHashMap<>();

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getConsumedOffset() {
        return consumedOffset;
    }

    public void updateOffsetTable(String topic, String group, Integer queueId, Long offset) {
        String key = topic + Constant.TOPIC_GROUP_SEPARATOR + group;
        consumedOffset.getOrDefault(key, new ConcurrentHashMap<>()).put(queueId, offset);
    }

    @Override
    public String encode() {
        return CommonCodec.encode2String(this);
    }

    @Override
    public void decode(String json) {
        ConsumerOffsetManager decode = CommonCodec.decode(json, ConsumerOffsetManager.class);
        this.consumedOffset = decode.consumedOffset;
    }

    @Override
    public String configFilePath() {
        return BrokerConfigHelper.consumerOffsetConfigPath();
    }
}
