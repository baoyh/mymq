package bao.study.mymq.broker.store;

import bao.study.mymq.broker.util.MappedFileHelper;

import java.nio.ByteBuffer;
import java.util.*;

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

    private final int queueId;

    private final int size = 8 + 4;

    private List<MappedFile> mappedFileList;

    public ConsumeQueue(String topic, int queueId) {
        this.topic = topic;
        this.queueId = queueId;
    }

    public List<ConsumeQueueOffset> pullMessage(long consumedQueueIndexOffset) {
        List<ConsumeQueueOffset> offsets = new ArrayList<>();
        long offset = consumedQueueIndexOffset * size;
        MappedFile mappedFile = MappedFileHelper.find(offset, mappedFileList);
        long fromOffset = mappedFile.getFileFromOffset();
        int position = (int) (offset - fromOffset);
        while (mappedFile.getWrotePosition().get() >= position + size) {
            ByteBuffer read = mappedFile.read(position, size);
            offsets.add(ConsumeQueueOffsetCodec.decode(read));
            position = position + size;
        }
        return offsets;
    }

    public void append(long offset, int size) {
        MappedFile mappedFile = MappedFileHelper.latestMappedFile(mappedFileList);
        mappedFile.appendConsumeQueueOffset(new ConsumeQueueOffset(offset, size));
    }

    public String getLastFileName() {
        return MappedFileHelper.latestMappedFile(mappedFileList).getFile().getName();
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setMappedFileList(List<MappedFile> mappedFileList) {
        this.mappedFileList = mappedFileList;
    }

    public int getSize() {
        return size;
    }
}
