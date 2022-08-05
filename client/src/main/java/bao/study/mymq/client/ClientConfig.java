package bao.study.mymq.client;

import bao.study.mymq.common.Constant;
import bao.study.mymq.remoting.RemotingUtil;

/**
 * @author baoyh
 * @since 2022/8/2 14:29
 */
public class ClientConfig {

    private String routerAddress = System.getProperty(Constant.ROUTER_ADDRESS_PROPERTY, System.getenv(Constant.ROUTER_ADDRESS_ENV));

    private String instanceName = System.getProperty(Constant.CLIENT_NAME_PROPERTY, "DEFAULT");

    private String clientIp = RemotingUtil.getLocalAddress();

    public String getRouterAddress() {
        return routerAddress;
    }

    public void setRouterAddress(String routerAddress) {
        this.routerAddress = routerAddress;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }
}
