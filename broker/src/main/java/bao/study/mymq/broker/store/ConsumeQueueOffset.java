package bao.study.mymq.broker.store;

/**
 * @author baoyh
 * @since 2023/5/24 11:20
 */
public class ConsumeQueueOffset {

    /**
     * commitlog offset
     */
    private Long offset;

    /**
     * message size
     */
    private Integer size;

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public ConsumeQueueOffset(Long offset, Integer size) {
        this.offset = offset;
        this.size = size;
    }
}
