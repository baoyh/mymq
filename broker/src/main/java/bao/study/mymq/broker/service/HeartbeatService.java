package bao.study.mymq.broker.service;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.common.ServiceThread;
import bao.study.mymq.common.protocol.broker.BrokerData;
import bao.study.mymq.common.protocol.broker.Heartbeat;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static bao.study.mymq.remoting.code.RequestCode.BROKER_HEARTBEAT;

/**
 * @author baoyh
 * @since 2024/6/11 16:04
 */
public class HeartbeatService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);

    private final RemotingClient client;

    private final BrokerProperties brokerProperties;

    private HeartbeatServiceCallback callback;

    private final Map<Long /* broker id */, String /* broker address */> addressMap = new ConcurrentHashMap<>();

    public HeartbeatService(BrokerController brokerController) {
        this.client = brokerController.getRemotingClient();
        this.brokerProperties = brokerController.getBrokerProperties();
    }

    public HeartbeatService(BrokerController brokerController, HeartbeatServiceCallback callback) {
        this(brokerController);
        this.callback = callback;
    }

    @Override
    public String getServiceName() {
        return HeartbeatService.class.getSimpleName();
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                this.sendHeartbeat();
            } catch (Throwable e) {
                log.error("Send heartbeat failed", e);
            } finally {
                waitForRunning(200);
            }
        }
    }

    public void sendHeartbeat() {
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(BROKER_HEARTBEAT,
                CommonCodec.encode(new Heartbeat(brokerProperties.getBrokerName(), brokerProperties.getBrokerId())));
        RemotingCommand response = client.invokeSync(brokerProperties.getRouterAddress(), remotingCommand, BrokerConfig.getRpcTimeoutMillis());
        BrokerData brokerData = CommonCodec.decode(response.getBody(), BrokerData.class);
        updateBrokerData(brokerData);
    }

    private void updateBrokerData(BrokerData brokerData) {
        addressMap.putAll(brokerData.getAddressMap());
        if (callback != null) {
            callback.onHeartbeatSuccess(addressMap);
        }
    }

    public Map<Long, String> getAddressMap() {
        return addressMap;
    }

    public interface HeartbeatServiceCallback {

        void onHeartbeatSuccess(Map<Long, String> addressMap);
    }
}
