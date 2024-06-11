package bao.study.mymq.broker;

/**
 * @author baoyh
 * @since 2024/6/7 13:43
 */
public class BrokerProperties {

    private String brokerName;

    private String clusterName;

    private String routerAddress;

    private int port;

    private long brokerId;

    public BrokerProperties(String brokerName, String clusterName, String routerAddress, int port, long brokerId) {
        this.brokerName = brokerName;
        this.clusterName = clusterName;
        this.routerAddress = routerAddress;
        this.port = port;
        this.brokerId = brokerId;
    }

    public BrokerProperties() {
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public String getRouterAddress() {
        return routerAddress;
    }

    public void setRouterAddress(String routerAddress) {
        this.routerAddress = routerAddress;
    }
}
