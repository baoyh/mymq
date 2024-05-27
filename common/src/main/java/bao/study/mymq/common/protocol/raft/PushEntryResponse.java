package bao.study.mymq.common.protocol.raft;

/**
 * @author baoyh
 * @since 2024/5/23 16:54
 */
public class PushEntryResponse extends BaseProtocol {

    private long index;

    private long beginIndex;

    private long endIndex;

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getBeginIndex() {
        return beginIndex;
    }

    public void setBeginIndex(long beginIndex) {
        this.beginIndex = beginIndex;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }
}
