package bao.study.mymq.common.protocol.broker;

/**
 * @author baoyh
 * @since 2024/6/12 15:27
 */
public class FlushMessage {

    private String topic;

    private int queueId;

    private long offset;

    private int size;

    public FlushMessage(String topic, int queueId, long offset, int size) {
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
        this.size = size;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

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
}
