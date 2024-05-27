package bao.study.mymq.broker.raft.store;

import bao.study.mymq.common.protocol.raft.RaftEntry;

import java.nio.ByteBuffer;

/**
 * @author baoyh
 * @since 2024/5/23 9:56
 */
public class RaftEntryCodec {

    public static ByteBuffer encode(RaftEntry raftEntry) {
        int size = raftEntry.computeSizeInBytes();
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(size);
        buffer.putLong(raftEntry.getIndex());
        buffer.putLong(raftEntry.getTerm());
        buffer.putLong(raftEntry.getPos());
        buffer.put(raftEntry.getBody());

        return buffer;
    }

    public static RaftEntry decode(ByteBuffer buffer) {

        try {

            RaftEntry raftEntry = new RaftEntry();

            raftEntry.setSize(buffer.getInt());
            raftEntry.setIndex(buffer.getLong());
            raftEntry.setTerm(buffer.getLong());
            raftEntry.setPos(buffer.getLong());
            byte[] body = new byte[raftEntry.getSize() - RaftEntry.HEADER_SIZE];
            buffer.get(body, 0 ,body.length);
            raftEntry.setBody(body);

            return raftEntry;

        } catch (Exception e) {
            buffer.position(buffer.limit());
        }

        return null;
    }
}
