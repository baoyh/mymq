package bao.study.mymq.common.protocol.body;

/**
 * @author baoyh
 * @since 2023/7/10 18:21
 */
public class SendMessageBackBody {

    private boolean status;

    private String group;

    private String topic;

    private int queueId;

    // consume queue index offset
    private long offset;

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
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
}
