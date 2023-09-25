package bao.study.mymq.common.protocol;

/**
 * @author baoyh
 * @since 2022/8/30 16:27
 */
public class MessageExt extends Message {

    private String brokerName;

    private String group;

    private int queueId;

    // consume queue index offset
    private long offset;

    private long bornTimeStamp;

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

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "MessageExt{" +
                "brokerName='" + brokerName + '\'' +
                ", group='" + group + '\'' +
                ", queueId=" + queueId +
                ", offset=" + offset +
                ", bornTimeStamp=" + bornTimeStamp +
                '}';
    }
}
