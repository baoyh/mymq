package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.store.ConsumeQueue;
import bao.study.mymq.broker.util.BrokerConfigHelper;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2023/5/5 20:50
 */
public class ConsumeQueueManager extends ConfigManager {

    ConcurrentHashMap<String, ConsumeQueue> consumeQueueTable = new ConcurrentHashMap<>();

    @Override
    public String encode() {
        return null;
    }

    @Override
    public void decode(String json) {

    }

    @Override
    public String configFilePath() {
        return BrokerConfigHelper.consumerOffsetConfigPath();
    }
}
