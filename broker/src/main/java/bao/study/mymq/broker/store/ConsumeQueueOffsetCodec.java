package bao.study.mymq.broker.store;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author baoyh
 * @since 2023/5/24 14:40
 */
public abstract class ConsumeQueueOffsetCodec {

    private static final Charset charset = StandardCharsets.UTF_8;

    public static ByteBuffer encode(ConsumeQueueOffset offset) {
        int size = 8 + 4;
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.putLong(offset.getOffset());
        byteBuffer.putInt(offset.getSize());
        return byteBuffer;
    }

    public static ConsumeQueueOffset decode(ByteBuffer buffer) {
        return new ConsumeQueueOffset(buffer.getLong(), buffer.getInt());
    }
}
