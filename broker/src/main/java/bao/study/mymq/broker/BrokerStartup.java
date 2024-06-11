package bao.study.mymq.broker;

import bao.study.mymq.broker.config.ConsumeQueueConfig;
import bao.study.mymq.broker.config.MessageStoreConfig;
import bao.study.mymq.broker.manager.CommitLogManager;
import bao.study.mymq.broker.manager.ConsumeQueueManager;
import bao.study.mymq.broker.manager.ConsumeQueueIndexManager;
import bao.study.mymq.broker.processor.SendMessageProcessor;
import bao.study.mymq.broker.raft.Config;
import bao.study.mymq.broker.raft.RaftServer;
import bao.study.mymq.broker.store.RaftCommitLog;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.body.RegisterBrokerBody;
import bao.study.mymq.common.protocol.broker.BrokerData;
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
import java.util.concurrent.ConcurrentHashMap;

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

    private static RaftServer raftServer;

    private static BrokerProperties brokerProperties;

    private static final Map<Long /* broker id */, String /* broker address */> addressMap = new ConcurrentHashMap<>();

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

    public static void start(int port, Map<String, Integer> topics) {
        startRemoting(port);
        registerBroker(topics);
        queryBrokers();
        initialize();
        registerRequestProcessor();
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

    private static void queryBrokers() {
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(QUERY_BROKERS_BY_BROKER_NAME, CommonCodec.encode(brokerProperties.getBrokerName()));
        RemotingCommand response = remotingClient.invokeSync(brokerProperties.getRouterAddress(), remotingCommand, 3000);
        BrokerData brokerData = CommonCodec.decode(response.getBody(), BrokerData.class);
        addressMap.putAll(brokerData.getAddressMap());
    }

    private static void initialize() {
        ConsumeQueueIndexManager consumeQueueIndexManager = new ConsumeQueueIndexManager();
        ConsumeQueueManager consumeQueueManager = new ConsumeQueueManager(new ConsumeQueueConfig());

        raftServer = new RaftServer(new Config(), getRaftNodes(), getSelfId(), remotingClient, remotingServer);
        CommitLogManager commitLogManager = new CommitLogManager(new RaftCommitLog(new MessageStoreConfig(), raftServer));
        brokerController = new BrokerController(consumeQueueIndexManager, consumeQueueManager, commitLogManager);

        brokerController.initialize();
        brokerController.start();
        raftServer.startup();

        registerRequestProcessor();
    }

    private static Map<String, String> getRaftNodes() {
        Map<String, String> nodes = new HashMap<>();
        addressMap.forEach((k, v) -> nodes.put(brokerProperties.getBrokerName() + Constant.RAFT_ID_SEPARATOR + k, v));
        return nodes;
    }

    private static String getSelfId() {
        return brokerProperties.getBrokerName() + Constant.RAFT_ID_SEPARATOR + brokerProperties.getBrokerId();
    }

    public static void shutdown() {
        remotingServer.shutdown();
        remotingClient.shutdown();
        raftServer.shutdown();
    }

    private static void registerRequestProcessor() {
        remotingServer.registerRequestProcessor(new SendMessageProcessor(brokerController), SEND_MESSAGE);
        remotingServer.registerRequestProcessor(brokerController.getPullMessageProcessor(), QUERY_CONSUMER_OFFSET, PULL_MESSAGE, CONSUMER_SEND_MSG_BACK);
    }

    public static void setBrokerProperties(BrokerProperties brokerProperties) {
        BrokerStartup.brokerProperties = brokerProperties;
    }
}
