package bao.study.mymq.common.protocol.broker;

/**
 * @author baoyh
 * @since 2024/6/17 15:17
 */
public class RegisterMaster {

    private String brokerName;

    private long masterId;

    public RegisterMaster(String brokerName, long masterId) {
        this.brokerName = brokerName;
        this.masterId = masterId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public long getMasterId() {
        return masterId;
    }

    public void setMasterId(long masterId) {
        this.masterId = masterId;
    }
}
