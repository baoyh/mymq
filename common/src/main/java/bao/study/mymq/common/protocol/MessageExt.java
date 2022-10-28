package bao.study.mymq.common.protocol;

/**
 * @author baoyh
 * @since 2022/8/30 16:27
 */
public class MessageExt extends Message {

    private String brokerName;

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
}
