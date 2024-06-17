package bao.study.mymq.broker;

import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.body.RegisterBrokerBody;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.RemotingUtil;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyClient;
import bao.study.mymq.remoting.netty.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static bao.study.mymq.remoting.code.RequestCode.*;

/**
 * @author baoyh
 * @since 2022/5/24 11:24
 */
public class BrokerStartup {

    private static final Logger log = LoggerFactory.getLogger(BrokerStartup.class);

    private static RemotingServer remotingServer;

    private static RemotingClient remotingClient;

    private static BrokerController brokerController;

    private static BrokerProperties brokerProperties;

    public static void main(String[] args) {

        try {

            int port = 10910;
            brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", port, Constant.MASTER_ID);

            Map<String, Integer> topics = new HashMap<>();
            topics.put("topic1", 4);
            topics.put("topic2", 4);

            start(port, topics);

            log.info("broker started");
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    public static BrokerController start(int port, Map<String, Integer> topics) {
        startRemoting(port);
        registerBroker(topics);
        initialize();
        registerRequestProcessor();
        return brokerController;
    }

    private static void startRemoting(int port) {
        remotingServer = new NettyServer(port);
        remotingServer.start();

        remotingClient = new NettyClient();
        remotingClient.start();
    }

    private static void registerBroker(Map<String, Integer> topics) {
        RegisterBrokerBody master = new RegisterBrokerBody();

        // 同一个 setClusterName - brokerName 中的不同 brokerId 的节点会被当做一个 raft 集群
        master.setBrokerId(brokerProperties.getBrokerId());
        master.setBrokerName(brokerProperties.getBrokerName());
        master.setClusterName(brokerProperties.getClusterName());
        master.setBrokerAddress(RemotingUtil.getLocalAddress() + ":" + brokerProperties.getPort());
        master.setTopics(topics);
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.REGISTER_BROKER, CommonCodec.encode(master));

        remotingClient.invokeOneway(brokerProperties.getRouterAddress(), remotingCommand, 3000);
    }

    private static void initialize() {
        brokerController = new BrokerController(remotingClient, remotingServer, brokerProperties);
        brokerController.initialize();
        brokerController.start();

        registerRequestProcessor();
    }

    public static void shutdown() {
        brokerController.shutdown();
    }

    private static void registerRequestProcessor() {
        remotingServer.registerRequestProcessor(brokerController.getSendMessageProcessor(), SEND_MESSAGE);
        remotingServer.registerRequestProcessor(brokerController.getPullMessageProcessor(), QUERY_CONSUMER_OFFSET, PULL_MESSAGE, CONSUMER_SEND_MSG_BACK);
        remotingServer.registerRequestProcessor(brokerController.getFlushSyncProcessor(), FLUSH_SYNC);
    }

    public static void setBrokerProperties(BrokerProperties brokerProperties) {
        BrokerStartup.brokerProperties = brokerProperties;
    }
}
