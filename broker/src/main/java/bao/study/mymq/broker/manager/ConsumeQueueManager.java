package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.config.ConsumeQueueConfig;
import bao.study.mymq.broker.store.ConsumeQueue;
import bao.study.mymq.broker.store.MappedFile;
import bao.study.mymq.common.Constant;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author baoyh
 * @since 2023/5/5 20:50
 */
public class ConsumeQueueManager extends ConfigManager {

    private ConsumeQueueConfig consumeQueueConfig;

    private final ConcurrentHashMap<String /*topic@queue*/, ConsumeQueue> consumeQueueTable = new ConcurrentHashMap<>();

    public ConsumeQueueManager(ConsumeQueueConfig consumeQueueConfig) {
        this.consumeQueueConfig = consumeQueueConfig;
    }

    public void updateConsumeQueueTable(String topic, int queueId, long offset, int size) {
        ConsumeQueue consumeQueue = consumeQueueTable.get(topic + Constant.TOPIC_SEPARATOR + queueId);
        if (consumeQueue == null) {
            List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();
            String fileName = configFilePath() + File.separator + topic + File.separator + queueId + File.separator + consumeQueueConfig.firstName();
            mappedFileList.add(new MappedFile(fileName, consumeQueueConfig.getSize()));
            consumeQueue = new ConsumeQueue(topic, queueId, mappedFileList);
        }

        consumeQueue.append(offset, size);
    }

    @Override
    public String encode() {
        return null;
    }

    @Override
    public void decode(String json) {

    }

    @Override
    public boolean load() {
        File root = new File(configFilePath());
        for (File topic : Objects.requireNonNull(root.listFiles())) {
            String topicName = topic.getName();
            for (File queue : Objects.requireNonNull(topic.listFiles())) {
                String queueId = queue.getName();
                List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();
                for (File index : Objects.requireNonNull(queue.listFiles())) {
                    mappedFileList.add(new MappedFile(configFilePath() + File.separator + topicName + File.separator + queueId + File.separator + index.getName(),
                            consumeQueueConfig.getSize()));
                }
                consumeQueueTable.put(topicName + Constant.TOPIC_SEPARATOR + queueId, new ConsumeQueue(topicName, Integer.parseInt(queueId), mappedFileList));
            }
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return consumeQueueConfig.getConsumeQueuePath();
    }

    public ConcurrentHashMap<String, ConsumeQueue> getConsumeQueueTable() {
        return consumeQueueTable;
    }
}
