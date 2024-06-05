package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.broker.config.ConsumeQueueConfig;
import bao.study.mymq.broker.store.ConsumeQueue;
import bao.study.mymq.broker.store.MappedFile;
import bao.study.mymq.broker.util.MessageStoreHelper;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.utils.CommonCodec;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author baoyh
 * @since 2023/5/5 20:50
 */
public class ConsumeQueueManager extends ConfigManager {

    private final transient ConsumeQueueConfig consumeQueueConfig;

    // 存放每一个 topic/queue 对应的 ConsumeQueue
    private final transient ConcurrentHashMap<String /*topic@queue*/, ConsumeQueue> consumeQueueTable = new ConcurrentHashMap<>();

    // 存放已经被提交的消息的偏移, 其中包括还未被消费过的部分
    private ConcurrentMap<String /*topic@queue*/, ConcurrentMap<String/* mappedFile name */, AtomicInteger/* committedPosition */>> committedTable = new ConcurrentHashMap<>();

    public ConsumeQueueManager(ConsumeQueueConfig consumeQueueConfig) {
        this.consumeQueueConfig = consumeQueueConfig;
    }

    /**
     * 消费提交后将消息消费偏移落盘
     */
    public void updateWhenMessageArriving(String topic, int queueId, long commitlogOffset, int size) {
        ConsumeQueue consumeQueue = getOrInitConsumeQueue(topic, queueId);
        consumeQueue.append(commitlogOffset, size);

        String fileName = consumeQueue.getLastFileName();
        checkCommittedTable(topic, queueId, fileName);
        committedTable.get(MessageStoreHelper.createKey(topic, queueId)).get(fileName).addAndGet(consumeQueue.getSize());

        commit();
    }

    private void checkCommittedTable(String topic, int queueId, String fileName) {
        String key = MessageStoreHelper.createKey(topic, queueId);
        if (committedTable.containsKey(key)) {
            if (!committedTable.get(key).containsKey(fileName)) {
                committedTable.get(key).put(fileName, new AtomicInteger());
            }
        } else {
            ConcurrentHashMap<String, AtomicInteger> map = new ConcurrentHashMap<>();
            map.put(fileName, new AtomicInteger());
            committedTable.put(key, map);
        }
    }

    private ConsumeQueue getOrInitConsumeQueue(String topic, int queueId) {
        String key = MessageStoreHelper.createKey(topic, queueId);
        ConsumeQueue consumeQueue = consumeQueueTable.get(key);
        if (consumeQueue == null) {
            List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();
            String fileName = consumeQueueConfig.getConsumeQueuePath() + File.separator + topic + File.separator + queueId + File.separator + consumeQueueConfig.firstName();
            mappedFileList.add(new MappedFile(fileName, consumeQueueConfig.getSize()));
            consumeQueue = new ConsumeQueue(topic, queueId);
            consumeQueue.setMappedFileList(mappedFileList);
            consumeQueueTable.put(key, consumeQueue);
        }
        return consumeQueue;
    }

    @Override
    public String encode() {
        return CommonCodec.encode2String(this);
    }

    @Override
    public void decode(String json) {
        if (json == null || json.isEmpty()) {
            return;
        }
        ConsumeQueueManager consumeQueueManager = CommonCodec.decode(json.getBytes(StandardCharsets.UTF_8), ConsumeQueueManager.class);
        this.committedTable = consumeQueueManager.getCommittedTable();
    }

    @Override
    public void load() {
        super.load();
        File consumeQueueFile = new File(consumeQueueConfig.getConsumeQueuePath());
        if (!consumeQueueFile.exists() || consumeQueueFile.listFiles().length == 0) {
            return;
        }
        for (File topic : Objects.requireNonNull(consumeQueueFile.listFiles())) {
            String topicName = topic.getName();
            for (File queue : Objects.requireNonNull(topic.listFiles())) {
                String queueId = queue.getName();
                String key = topicName + Constant.TOPIC_SEPARATOR + queueId;
                if (!committedTable.containsKey(key)) {
                    continue;
                }

                ConcurrentMap<String, AtomicInteger> table = committedTable.get(key);
                List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();
                table.forEach((name, position) -> mappedFileList.add(new MappedFile(consumeQueueConfig.getConsumeQueuePath() + File.separator + topicName + File.separator + queueId + File.separator + name,
                        consumeQueueConfig.getSize(), position.get())));

                ConsumeQueue consumeQueue = new ConsumeQueue(topicName, Integer.parseInt(queueId));
                consumeQueue.setMappedFileList(mappedFileList);
                consumeQueueTable.put(key, consumeQueue);
            }
        }
    }

    @Override
    public String configFilePath() {
        return BrokerConfig.consumeQueueConfigPath();
    }

    public ConcurrentHashMap<String, ConsumeQueue> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> getCommittedTable() {
        return committedTable;
    }
}
