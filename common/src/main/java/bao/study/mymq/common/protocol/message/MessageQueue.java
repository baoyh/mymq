package bao.study.mymq.common.protocol.message;

/**
 * @author baoyh
 * @since 2022/6/30 16:44
 */
public class MessageQueue {

    private String brokerName;

    private String topic;

    private int queueId;

    public MessageQueue(String brokerName, String topic, int queueId) {
        this.brokerName = brokerName;
        this.topic = topic;
        this.queueId = queueId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
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
