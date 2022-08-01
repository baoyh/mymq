package bao.study.mymq.common.protocol;

/**
 * @author baoyh
 * @since 2022/6/30 19:32
 */
public class TopicData {

    private String topic;

    private int writeQueueNums = 4;

    private int readQueueNums = 4;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }
}
