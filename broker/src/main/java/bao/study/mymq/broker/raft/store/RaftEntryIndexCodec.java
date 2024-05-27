package bao.study.mymq.broker.raft.store;

import java.nio.ByteBuffer;

/**
 * @author baoyh
 * @since 2024/5/23 9:56
 */
public class RaftEntryIndexCodec {

    public static ByteBuffer encode(RaftEntryIndex entryIndex) {
        int indexSize = 4 + 8 + 8 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(indexSize);

        buffer.putInt(entryIndex.getSize());
        buffer.putLong(entryIndex.getPos());
        buffer.putLong(entryIndex.getIndex());
        buffer.putLong(entryIndex.getTerm());

        return buffer;
    }

    public static RaftEntryIndex decode(ByteBuffer buffer) {

        try {

            RaftEntryIndex entryIndex = new RaftEntryIndex();
            entryIndex.setSize(buffer.getInt());
            entryIndex.setPos(buffer.getLong());
            entryIndex.setIndex(buffer.getLong());
            entryIndex.setTerm(buffer.getLong());

            return entryIndex;
        } catch (Exception e) {
            buffer.position(buffer.limit());
        }

        return null;
    }
}
