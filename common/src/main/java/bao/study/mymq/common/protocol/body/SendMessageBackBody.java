package bao.study.mymq.common.protocol.body;

/**
 * @author baoyh
 * @since 2023/7/10 18:21
 */
public class SendMessageBackBody {

    private boolean status;

    private String topic;

    private int queueId;

    private long commitlogOffset;

    private int size;

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
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

    public long getCommitlogOffset() {
        return commitlogOffset;
    }

    public void setCommitlogOffset(long commitlogOffset) {
        this.commitlogOffset = commitlogOffset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
