package bao.study.mymq.broker.service;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.common.protocol.broker.RegisterMaster;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;

/**
 * @author baoyh
 * @since 2024/6/17 15:11
 */
public class MasterManagerService {

    private final RemotingClient remotingClient;

    private final BrokerProperties brokerProperties;

    public MasterManagerService(BrokerController brokerController) {
        this.remotingClient = brokerController.getRemotingClient();
        this.brokerProperties = brokerController.getBrokerProperties();
    }

    public void registerMaster() {
        RemotingCommand registerMaster = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.REGISTER_MASTER,
                CommonCodec.encode(new RegisterMaster(brokerProperties.getBrokerName(), brokerProperties.getBrokerId())));
        remotingClient.invokeOneway(brokerProperties.getRouterAddress(), registerMaster, BrokerConfig.getRpcTimeoutMillis());
    }
}
