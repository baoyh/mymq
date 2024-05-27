package bao.study.mymq.common.protocol.raft;

/**
 * @author baoyh
 * @since 2024/5/21 14:43
 */
public class AppendEntryResponse extends BaseProtocol {

    private long pos = -1;

    private long index = -1;

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "AppendEntryResponse{" +
                "pos=" + pos +
                ", index=" + index +
                '}';
    }
}
