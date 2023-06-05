package bao.study.mymq.broker.store;

/**
 * @author baoyh
 * @since 2023/5/24 11:20
 */
public class ConsumeQueueOffset {

    /**
     * commitlog offset
     */
    private long offset;

    /**
     * message size
     */
    private int size;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ConsumeQueueOffset(long offset, int size) {
        this.offset = offset;
        this.size = size;
    }

    public ConsumeQueueOffset() {
    }
}
