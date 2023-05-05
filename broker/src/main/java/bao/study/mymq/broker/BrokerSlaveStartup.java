package bao.study.mymq.broker;

import bao.study.mymq.broker.manager.ConsumerOffsetManager;
import bao.study.mymq.broker.processor.ConsumerManageProcessor;
import bao.study.mymq.broker.processor.SendMessageProcessor;
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

/**
 * @author baoyh
 * @since 2022/5/24 11:24
 */
public class BrokerSlaveStartup {

    private static final Logger log = LoggerFactory.getLogger(BrokerSlaveStartup.class);

    private static RemotingServer remotingServer;

    private static RemotingClient remotingClient;

    private static BrokerController brokerController;

    public static void main(String[] args) {

        try {

            int port = 10911;
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
            master.setBrokerId(1);
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
        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager();
        brokerController = new BrokerController(consumerOffsetManager);

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
        remotingServer.registerRequestProcessor(new SendMessageProcessor(), RequestCode.SEND_MESSAGE);
        remotingServer.registerRequestProcessor(new ConsumerManageProcessor(brokerController), RequestCode.QUERY_CONSUMER_OFFSET);
    }


}