package bao.study.mymq.broker.store;

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

    private String topic;

    private int queue;

    private final int size = 8 + 4;

    private List<MappedFile> mappedFileList;

    public ConsumeQueue(String topic, int queue, List<MappedFile> mappedFileList) {
        this.topic = topic;
        this.queue = queue;
        this.mappedFileList = mappedFileList;
    }

    public List<ConsumeQueueOffset> pullMessage(long consumedOffset) {
        List<ConsumeQueueOffset> offsets = new ArrayList<>();
        long offset = consumedOffset * size;
        MappedFile mappedFile = MappedFileHelper.find(offset, mappedFileList);
        while (mappedFile.wrotePosition.get() > offset - mappedFile.fileFromOffset) {
            ByteBuffer read = mappedFile.read((int) offset, size);
            offsets.add(ConsumeQueueOffsetCodec.decode(read));
        }
        return offsets;
    }


}
