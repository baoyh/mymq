package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.store.ConsumeQueue;
import bao.study.mymq.broker.store.MappedFile;
import bao.study.mymq.broker.util.BrokerConfig;
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

    ConcurrentHashMap<String /*topic@queue*/, ConsumeQueue> consumeQueueTable = new ConcurrentHashMap<>();

    public void updateConsumeQueueTable(String topic, int queueId, long offset, int size) {

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
                String queueName = queue.getName();
                List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();
                for (File index : Objects.requireNonNull(queue.listFiles())) {
                    mappedFileList.add(new MappedFile(index.getName(), index.length()));
                }
                consumeQueueTable.put(topicName + Constant.TOPIC_SEPARATOR + queueName, new ConsumeQueue(topicName, Integer.parseInt(queueName), mappedFileList));
            }
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return BrokerConfig.consumeQueuePath();
    }

    public ConcurrentHashMap<String, ConsumeQueue> getConsumeQueueTable() {
        return consumeQueueTable;
    }
}
