package bao.study.mymq.broker.service;


import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.common.protocol.broker.FlushMessage;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 只在 master 端使用
 *
 * @author baoyh
 * @since 2024/6/12 15:18
 */
public class FlushSyncService {

    private static final Logger log = LoggerFactory.getLogger(FlushSyncService.class);

    private final RemotingClient remotingClient;

    private final BrokerProperties brokerProperties;

    private final BrokerController brokerController;

    public FlushSyncService(BrokerController brokerController) {
        this.brokerProperties = brokerController.getBrokerProperties();
        this.remotingClient = brokerController.getRemotingClient();
        this.brokerController = brokerController;
    }

    /**
     * 同步刷新 slave 的配置
     */
    public void flushSync(String topic, int queueId, long offset, int size) {
        brokerController.getHeartbeatService().getAddressMap().forEach((brokerId, brokerAddress) -> {
            try {
                if (brokerId != brokerProperties.getBrokerId()) {
                    RemotingCommand flushSync = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.FLUSH_SYNC, CommonCodec.encode(new FlushMessage(topic, queueId, offset, size)));
                    remotingClient.invokeOneway(brokerAddress, flushSync, BrokerConfig.getRpcTimeoutMillis());
                }
            } catch (Throwable e) {
                log.error("flush {} fail", brokerAddress, e);
            }
        });

    }
}
