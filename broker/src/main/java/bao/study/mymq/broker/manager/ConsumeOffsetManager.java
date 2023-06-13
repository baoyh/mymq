package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.utils.CommonCodec;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author baoyh
 * @since 2022/10/25 17:15
 */
public class ConsumeOffsetManager extends ConfigManager {

    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer/* queueId */, Long/* offset */>> consumedOffset = new ConcurrentHashMap<>();

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getConsumedOffset() {
        return consumedOffset;
    }

    public void updateConsumedOffset(String topic, String group, Integer queueId, Long offset) {
        String key = topic + Constant.TOPIC_SEPARATOR + group;
        ConcurrentMap<Integer, Long> consumed = consumedOffset.getOrDefault(key, new ConcurrentHashMap<>());
        offset = offset + consumed.getOrDefault(queueId, 0L);
        consumed.put(queueId, offset);

        // TODO asynchronous
        commit();
    }

    @Override
    public String encode() {
        return CommonCodec.encode2String(this);
    }

    @Override
    public void decode(String json) {
        ConsumeOffsetManager decode = CommonCodec.decode(json, ConsumeOffsetManager.class);
        this.consumedOffset = decode.consumedOffset;
    }

    @Override
    public String configFilePath() {
        return BrokerConfig.consumerOffsetConfigPath();
    }

}
