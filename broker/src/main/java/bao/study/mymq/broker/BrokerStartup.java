package bao.study.mymq.broker;

import bao.study.mymq.broker.config.ConsumeQueueConfig;
import bao.study.mymq.broker.config.MessageStoreConfig;
import bao.study.mymq.broker.manager.CommitLogManager;
import bao.study.mymq.broker.manager.ConsumeQueueManager;
import bao.study.mymq.broker.manager.ConsumeQueueOffsetManager;
import bao.study.mymq.broker.processor.ConsumeManageProcessor;
import bao.study.mymq.broker.processor.SendMessageProcessor;
import bao.study.mymq.broker.store.CommitLog;
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

    public static void main(String[] args) {

        try {

            int port = 10910;
            remotingServer = new NettyServer(port);
            remotingServer.start();

            remotingClient = new NettyClient();
            remotingClient.start();

            initialize();

            registerRequestProcessor();

            Map<String, Integer> topics = new HashMap<>();
            topics.put("topic1", 4);
            topics.put("topic2", 4);

            RegisterBrokerBody master = new RegisterBrokerBody();
            master.setBrokerId(Constant.MASTER_ID);
            master.setBrokerName("broker1");
            master.setClusterName("cluster1");
            master.setBrokerAddress(RemotingUtil.getLocalAddress() + ":" + port);
            master.setTopics(topics);
            RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.REGISTER_BROKER, CommonCodec.encode(master));

            remotingClient.invokeOneway("localhost:9875", remotingCommand, 3000);

            log.info("broker started");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void initialize() {
        ConsumeQueueOffsetManager consumeQueueOffsetManager = new ConsumeQueueOffsetManager();
        ConsumeQueueManager consumeQueueManager = new ConsumeQueueManager(new ConsumeQueueConfig());
        CommitLogManager commitLogManager = new CommitLogManager(new CommitLog(new MessageStoreConfig()));
        brokerController = new BrokerController(consumeQueueOffsetManager, consumeQueueManager, commitLogManager);

        boolean initialize = brokerController.initialize();
        if (!initialize) {
            shutdown();
            System.exit(1);
        }

        registerRequestProcessor();
    }

    private static void shutdown() {
        remotingServer.shutdown();
        remotingClient.shutdown();
    }

    private static void registerRequestProcessor() {
        remotingServer.registerRequestProcessor(new SendMessageProcessor(brokerController), SEND_MESSAGE);
        remotingServer.registerRequestProcessor(new ConsumeManageProcessor(brokerController), QUERY_CONSUMER_OFFSET, PULL_MESSAGE);
    }


}
