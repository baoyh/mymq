package bao.study.mymq.broker.store;

import bao.study.mymq.broker.util.MappedFileHelper;
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
//        ByteBuffer read = mappedFile.read(0, 12);
//        ConsumeQueueOffset decode = ConsumeQueueOffsetCodec.decode(read);
//        read = mappedFile.read(12, 12);
//        decode = ConsumeQueueOffsetCodec.decode(read);
//        System.out.println(decode);

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
}
