package bao.study.mymq.broker.store;

import bao.study.mymq.broker.raft.store.RaftEntryCodec;
import bao.study.mymq.broker.util.MappedFileHelper;
import bao.study.mymq.common.protocol.raft.RaftEntry;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author baoyh
 * @since 2024/6/5 15:15
 */
public class MappedFileTest {

    @Test
    public void readTest() {
        MappedFile mappedFile = new MappedFile("D:\\Work\\code\\mymq\\store\\consumequeue\\topic1\\0\\00000000", 12 * 1024);
        mappedFile.setWrotePosition(24);

        long consumedQueueIndexOffset = 0L;
        int size = 12;
        List<ConsumeQueueOffset> offsets = new ArrayList<>();
        long offset = consumedQueueIndexOffset * size;
        long fromOffset = mappedFile.getFileFromOffset();
        int position = (int) (offset - fromOffset);
        while (mappedFile.getWrotePosition().get() >= position + size) {
            ByteBuffer read = mappedFile.read(position, size);
            offsets.add(ConsumeQueueOffsetCodec.decode(read));
            position = position + size;
        }
        System.out.println(offsets);
    }

    @Test
    public void readCommitlog() {
        MappedFile mappedFile = new MappedFile("D:\\Work\\code\\mymq\\raftstore\\raft-broker1@0\\data\\00000000", 12 * 1024 * 1024);
        ByteBuffer read = mappedFile.read(0, 103);
        RaftEntry decode = RaftEntryCodec.decode(read);
        assert decode != null;
        ByteBuffer put = ByteBuffer.allocate(decode.getBody().length).put(decode.getBody());
        put.flip();
        MessageStore messageStore = MessageStoreCodec.decode(put);
        System.out.println(messageStore);
    }
}
