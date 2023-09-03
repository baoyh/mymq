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

    private final transient ConcurrentHashMap<String /*topic@queue*/, ConsumeQueue> consumeQueueTable = new ConcurrentHashMap<>();

    private ConcurrentMap<String /*topic@queue*/, ConcurrentMap<String/* mappedFile name */, AtomicInteger/* committedPosition */>> committedTable;

    public ConsumeQueueManager(ConsumeQueueConfig consumeQueueConfig) {
        this.consumeQueueConfig = consumeQueueConfig;
    }

    public void updateConsumeQueue(String topic, int queueId, long offset, int size) {
        String key = MessageStoreHelper.createKey(topic, queueId);
        ConsumeQueue consumeQueue = consumeQueueTable.get(key);
        if (consumeQueue == null) {
            List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();
            String fileName = configFilePath() + File.separator + topic + File.separator + queueId + File.separator + consumeQueueConfig.firstName();
            mappedFileList.add(new MappedFile(fileName, consumeQueueConfig.getSize()));
            consumeQueue = new ConsumeQueue(topic, queueId);
            consumeQueue.setMappedFileList(mappedFileList);
        }

        consumeQueue.append(offset, size);

        String fileName = consumeQueue.getLastFileName();
        committedTable.get(key).get(fileName).addAndGet(consumeQueue.getSize());

        commit();
    }

    @Override
    public String encode() {
        return CommonCodec.encode2String(this);
    }

    @Override
    public void decode(String json) {
        ConsumeQueueManager consumeQueueManager = CommonCodec.decode(json.getBytes(StandardCharsets.UTF_8), ConsumeQueueManager.class);
        this.committedTable = consumeQueueManager.getCommittedTable();
    }

    @Override
    public boolean load() {
        super.load();
        File consumeQueueFile = new File(consumeQueueConfig.getConsumeQueuePath());
        for (File topic : Objects.requireNonNull(consumeQueueFile.listFiles())) {
            String topicName = topic.getName();
            for (File queue : Objects.requireNonNull(topic.listFiles())) {
                String queueId = queue.getName();
                String key = topicName + Constant.TOPIC_SEPARATOR + queueId;

                List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();
                ConcurrentMap<String, AtomicInteger> table = committedTable.get(key);
                table.forEach((name, position) -> mappedFileList.add(new MappedFile(consumeQueueConfig.getConsumeQueuePath() + File.separator + topicName + File.separator + queueId + File.separator + name,
                        consumeQueueConfig.getSize(), position.get())));

                ConsumeQueue consumeQueue = new ConsumeQueue(topicName, Integer.parseInt(queueId));
                consumeQueue.setMappedFileList(mappedFileList);
                consumeQueueTable.put(key, consumeQueue);
            }
        }
        return true;
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
