package bao.study.mymq.common.protocol;

/**
 * @author baoyh
 * @since 2022/8/2 13:50
 */
public class Message {

    private String topic;

    private byte[] body;

    public Message(String topic, byte[] body) {
        this.topic = topic;
        this.body = body;
    }

    public Message() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
