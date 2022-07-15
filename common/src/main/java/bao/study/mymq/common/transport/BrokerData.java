package bao.study.mymq.common.transport;

import java.util.HashMap;
import java.util.Map;

/**
 * @author baoyh
 * @since 2022/6/30 16:38
 */
public class BrokerData {

    private String clusterName;

    private String storeName;

    private Map<Long /* broker id */, String /* broker address */> addressMap = new HashMap<>();

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public Map<Long, String> getAddressMap() {
        return addressMap;
    }

    public void setAddressMap(Map<Long, String> addressMap) {
        this.addressMap = addressMap;
    }
}
