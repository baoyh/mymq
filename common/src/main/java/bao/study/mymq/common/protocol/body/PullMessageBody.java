package bao.study.mymq.common.protocol.body;

/**
 * @author baoyh
 * @since 2023/2/4 14:47
 */
public class PullMessageBody {

    private String topic;

    private String group;

    private int queueId;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
}
