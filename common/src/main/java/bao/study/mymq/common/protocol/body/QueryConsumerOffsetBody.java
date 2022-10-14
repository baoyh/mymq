package bao.study.mymq.common.protocol.body;

/**
 * @author baoyh
 * @since 2022/10/14 10:35
 */
public class QueryConsumerOffsetBody {

    private String group;

    private String topic;

    private int queueId;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
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
}
