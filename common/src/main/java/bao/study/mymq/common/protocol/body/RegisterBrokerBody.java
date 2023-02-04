package bao.study.mymq.common.protocol.body;

import java.util.Map;

/**
 * @author baoyh
 * @since 2022/7/18 14:41
 */
public class RegisterBrokerBody {

    private String brokerName;

    private long brokerId;

    private String clusterName;

    private String brokerAddress;

    private Map<String /* topic */, Integer /* queueNums */> topics;

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

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public Map<String, Integer> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, Integer> topics) {
        this.topics = topics;
    }

    @Override
    public String toString() {
        return "RegisterBrokerBody{" +
                "brokerName='" + brokerName + '\'' +
                ", brokerId=" + brokerId +
                ", clusterName='" + clusterName + '\'' +
                ", brokerAddress='" + brokerAddress + '\'' +
                ", topics=" + topics +
                '}';
    }
}
