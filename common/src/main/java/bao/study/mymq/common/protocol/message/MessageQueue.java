package bao.study.mymq.common.protocol.message;

/**
 * @author baoyh
 * @since 2022/6/30 16:44
 */
public class MessageQueue {

    private String brokerName;

    private String topic;

    public MessageQueue(String brokerName, String topic) {
        this.brokerName = brokerName;
        this.topic = topic;
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

}
