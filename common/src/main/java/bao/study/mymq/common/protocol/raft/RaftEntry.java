package bao.study.mymq.common.protocol.raft;

import java.util.Arrays;

/**
 * @author baoyh
 * @since 2024/5/21 16:31
 */
public class RaftEntry {

    public final static int HEADER_SIZE = 4 + 8 + 8 + 8;

    /**
     * 条目总长度，包含 Header(协议头) + 消息体，占 4 字节
     */
    private int size;
    /**
     * 当前条目的 index，占 8 字节
     */
    private long index;
    /**
     * 当前条目所属的 投票轮次，占 8 字节
     */
    private long term;
    /**
     * 该条目的物理偏移量，类似于 commitlog 文件的物理偏移量，占 8 字节
     */
    private long pos; //used to validate data
    /**
     * 具体消息的内容
     */
    private byte[] body;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }


    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }


    public int computeSizeInBytes() {
        size = HEADER_SIZE + body.length;
        return size;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    @Override
    public boolean equals(Object entry) {
        if (!(entry instanceof RaftEntry)) {
            return false;
        }
        RaftEntry other = (RaftEntry) entry;
        if (this.size != other.size
                || this.index != other.index
                || this.term != other.term
                || this.pos != other.pos) {
            return false;
        }
        if (body == null) {
            return other.body == null;
        }

        if (other.body == null) {
            return false;
        }
        if (body.length != other.body.length) {
            return false;
        }
        for (int i = 0; i < body.length; i++) {
            if (body[i] != other.body[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int h = 1;
        h = prime * h + size;
        h = prime * h + (int) index;
        h = prime * h + (int) term;
        h = prime * h + (int) pos;
        if (body != null) {
            for (byte b : body) {
                h = prime * h + b;
            }
        } else {
            h = prime * h;
        }
        return h;
    }

    @Override
    public String toString() {
        return "RaftEntry{" +
                "size=" + size +
                ", index=" + index +
                ", term=" + term +
                ", pos=" + pos +
                ", body=" + Arrays.toString(body) +
                '}';
    }
}
