package bao.study.mymq.broker.store;

import bao.study.mymq.broker.config.ConsumeQueueConfig;
import bao.study.mymq.broker.util.MappedFileHelper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * ConsumeQueue 用于提高消费的速度, 作用是充当消息在 CommitLog 中的索引
 * 消费时根据 Topic/Queue/Index 找到对应的索引文件
 * 文件中每条数据都被设计成定长 commitlog offset(8) + message size(4)
 *
 * @author baoyh
 * @since 2023/5/9 14:06
 */
public class ConsumeQueue {

    private final String topic;

    private final int queue;

    private final int size = 8 + 4;

    private String lastFileName;

    private List<MappedFile> mappedFileList;

    public ConsumeQueue(String topic, int queue) {
        this.topic = topic;
        this.queue = queue;
    }

    public List<ConsumeQueueOffset> pullMessage(long consumedOffset) {
        List<ConsumeQueueOffset> offsets = new ArrayList<>();
        long offset = consumedOffset * size;
        MappedFile mappedFile = MappedFileHelper.find(offset, mappedFileList);
        long fromOffset = mappedFile.getFileFromOffset();
        while (offset >= fromOffset && mappedFile.wrotePosition.get() >= offset - fromOffset) {
            ByteBuffer read = mappedFile.read((int) offset, size);
            offsets.add(ConsumeQueueOffsetCodec.decode(read));
            fromOffset = fromOffset + size;
        }
        return offsets;
    }

    public void append(long offset, int size) {
        MappedFile mappedFile = MappedFileHelper.find(offset, mappedFileList);
        mappedFile.appendConsumeQueueOffset(new ConsumeQueueOffset(offset, size));
    }

    public String getLastFileName() {
        return String.valueOf(MappedFileHelper.latestMappedFile(mappedFileList).getFileFromOffset());
    }

    public String getTopic() {
        return topic;
    }

    public int getQueue() {
        return queue;
    }

    public void setMappedFileList(List<MappedFile> mappedFileList) {
        this.mappedFileList = mappedFileList;
    }

}
