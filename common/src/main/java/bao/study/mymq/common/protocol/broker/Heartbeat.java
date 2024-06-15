package bao.study.mymq.common.protocol.broker;

/**
 * @author baoyh
 * @since 2024/6/13 16:46
 */
public class Heartbeat {

    private String brokerName;

    private long brokerId;

    public Heartbeat(String brokerName, long brokerId) {
        this.brokerName = brokerName;
        this.brokerId = brokerId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }
}
