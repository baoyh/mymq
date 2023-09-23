package bao.study.mymq.common.protocol;

/**
 * @author baoyh
 * @since 2022/8/30 16:27
 */
public class MessageExt extends Message {

    private String brokerName;

    private int queueId;

    private long commitlogOffset;

    private long bornTimeStamp;

    private int size;

    public MessageExt() {
        super();
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public long getBornTimeStamp() {
        return bornTimeStamp;
    }

    public void setBornTimeStamp(long bornTimeStamp) {
        this.bornTimeStamp = bornTimeStamp;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getCommitlogOffset() {
        return commitlogOffset;
    }

    public void setCommitlogOffset(long commitlogOffset) {
        this.commitlogOffset = commitlogOffset;
    }

    @Override
    public String toString() {
        return "MessageExt{" +
                "brokerName='" + brokerName + '\'' +
                ", queueId=" + queueId +
                ", bornTimeStamp=" + bornTimeStamp +
                '}';
    }
}
