package bao.study.mymq.broker.store;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * @author baoyh
 * @since 2022/7/15 14:25
 */
public class Message {

    private int size;

    private String brokerName;

    private String topic;

    private int queueId;

    private long commitLogOffset;

    private InetSocketAddress bornHost;

    private long bornTimeStamp;

    private long storeTimeStamp;

    private byte[] body;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
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

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public InetSocketAddress getBornHost() {
        return bornHost;
    }

    public void setBornHost(InetSocketAddress bornHost) {
        this.bornHost = bornHost;
    }

    public long getBornTimeStamp() {
        return bornTimeStamp;
    }

    public void setBornTimeStamp(long bornTimeStamp) {
        this.bornTimeStamp = bornTimeStamp;
    }

    public long getStoreTimeStamp() {
        return storeTimeStamp;
    }

    public void setStoreTimeStamp(long storeTimeStamp) {
        this.storeTimeStamp = storeTimeStamp;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "Message{" +
                "size=" + size +
                ", brokerName='" + brokerName + '\'' +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", commitLogOffset=" + commitLogOffset +
                ", bornHost=" + bornHost +
                ", bornTimeStamp=" + bornTimeStamp +
                ", storeTimeStamp=" + storeTimeStamp +
                ", body=" + Arrays.toString(body) +
                '}';
    }
}
