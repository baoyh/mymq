package bao.study.mymq.common.protocol.broker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author baoyh
 * @since 2022/6/30 16:38
 */
public class BrokerData {

    private String clusterName;

    private String brokerName;

    private long masterId;

    private Map<Long /* broker id */, String /* broker address */> addressMap = new HashMap<>();

    public BrokerData(String clusterName, String brokerName) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public Map<Long, String> getAddressMap() {
        return addressMap;
    }

    public void setAddressMap(Map<Long, String> addressMap) {
        this.addressMap = addressMap;
    }

    public long getMasterId() {
        return masterId;
    }

    public void setMasterId(long masterId) {
        this.masterId = masterId;
    }
}
