package bao.study.mymq.broker.raft.store;

/**
 * @author baoyh
 * @since 2024/5/23 13:44
 */
public class RaftEntryIndex {

    public static final int INDEX_UNIT_SIZE = 28;

    /**
     * entry data size
     */
    private int size;

    /**
     * position in RaftEntry‘s MappedFile
     */
    private long pos;

    /**
     * index of entry
     */
    private long index;

    /**
     * term of raft
     */
    private long term;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

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

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }
}
