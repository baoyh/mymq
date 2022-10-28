package bao.study.mymq.client;

import bao.study.mymq.common.Constant;
import bao.study.mymq.common.ServiceState;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingUtil;
import bao.study.mymq.remoting.netty.NettyClient;

/**
 * @author baoyh
 * @since 2022/8/2 14:29
 */
public abstract class Client {

    protected final RemotingClient remotingClient = new NettyClient();

    private String routerAddress = System.getProperty(Constant.ROUTER_ADDRESS_PROPERTY, System.getenv(Constant.ROUTER_ADDRESS_ENV));

    private String instanceName = System.getProperty(Constant.CLIENT_NAME_PROPERTY, "DEFAULT");

    private String clientIp = RemotingUtil.getLocalAddress();

    private ServiceState serviceState = ServiceState.JUST_START;

    public void start() {
        switch (serviceState) {
            case JUST_START:
                try {
                    remotingClient.start();
                    doStart();
                    serviceState = ServiceState.RUNNING;

                } catch (Exception e) {
                    serviceState = ServiceState.START_FAIL;
                    throw new ClientException("client start fail", e);
                }
                break;
            case START_FAIL:
                throw new ClientException("client start fail");
            default:
                break;
        }
    }

    protected abstract void doStart();

    public void shutdown() {
        serviceState = ServiceState.SHUTDOWN;
        remotingClient.shutdown();
        doShutdown();
    }

    protected abstract void doShutdown();

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
