package bao.study.mymq.broker.store;

import bao.study.mymq.broker.BrokerException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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

    private Integer queue;

    private Integer size = 8 + 4;

    private List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();

    public List<ConsumeQueueOffset> pullMessage(long consumedOffset) {
        long offset = consumedOffset * size;
        MappedFile mappedFile = findMappedFile(offset);
        while (mappedFile.wrotePosition.get() > offset - mappedFile.fileFromOffset) {
            ByteBuffer read = mappedFile.read((int) offset, size);
            ConsumeQueueOffset consumeQueueOffset = ConsumeQueueOffsetCodec.decode(read);
        }
        return null;
    }

    private MappedFile findMappedFile(long offset) {
        for (int i = 0; i < mappedFileList.size(); i++) {
            if (mappedFileList.get(i).fileFromOffset > offset) {
                return mappedFileList.get(Math.max((i - 1), 0));
            }
        }
        throw new BrokerException("Cannot find the mapped file by offset [" + offset + "]");
    }
}
