package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.utils.CommonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 对于 ConsumeQueue 的相关缓存管理
 *
 * @author baoyh
 * @since 2022/10/25 17:15
 */
public class ConsumeQueueIndexManager extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(ConsumeQueueIndexManager.class);

    private final transient BrokerProperties brokerProperties;

    // 存放每个 topic/group/queueId 中消费完的消息的最新偏移
    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer/* queueId */, Long/* offset */>> consumedOffset = new ConcurrentHashMap<>();

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getConsumedOffset() {
        return consumedOffset;
    }

    public ConsumeQueueIndexManager(BrokerProperties brokerProperties) {
        this.brokerProperties = brokerProperties;
    }

    public synchronized void updateConsumedOffset(String topic, String group, Integer queueId, Long offset) {
        log.info("updateConsumedOffset: brokerId={}, topic={}, group={}, queueId={}, offset={}", brokerProperties.getBrokerId(), topic, group, queueId, offset);
        String key = topic + Constant.TOPIC_SEPARATOR + group;
        checkConsumedOffset(topic, group, queueId);
        ConcurrentMap<Integer, Long> consumed = consumedOffset.get(key);
        consumed.put(queueId, offset);

        // TODO asynchronous
        commit();
        log.info("after update: brokerId={}, consumedOffset: {}", brokerProperties.getBrokerId(), consumedOffset);
    }

    public void checkConsumedOffset(String topic, String group, int queueId) {
        ConcurrentHashMap<Integer, Long> map = new ConcurrentHashMap<>();
        map.put(queueId, 0L);
        String key = topic + Constant.TOPIC_SEPARATOR + group;
        if (consumedOffset.containsKey(key)) {
            ConcurrentMap<Integer, Long> consumed = consumedOffset.get(key);
            if (!consumed.containsKey(queueId)) {
                consumed.put(queueId, 0L);
            }
        } else {
            consumedOffset.put(key, map);
        }
    }

    @Override
    public String encode() {
        return CommonCodec.encode2String(this);
    }

    @Override
    public void decode(String json) {
        if (json == null || json.isEmpty()) return;
        ConsumeQueueIndexManager decode = CommonCodec.decode(json, ConsumeQueueIndexManager.class);
        this.consumedOffset = decode.consumedOffset;
    }

    @Override
    public String configFilePath() {
        return BrokerConfig.getConfigRootPath() + File.separator + brokerProperties.getBrokerName() + File.separator + brokerProperties.getBrokerId() + File.separator + BrokerConfig.consumeQueueOffsetConfigName();
    }

}
