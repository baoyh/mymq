package bao.study.mymq.broker;

import bao.study.mymq.broker.processor.SendMessageProcessor;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.body.RegisterBrokerBody;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyClient;
import bao.study.mymq.remoting.netty.NettyServer;

import java.util.HashSet;

/**
 * @author baoyh
 * @since 2022/5/24 11:24
 */
public class BrokerStartup {

    public static void main(String[] args) {

        try {
            int port = 10910;
            RemotingServer remotingServer = new NettyServer(port);
            registerRequestProcessor(remotingServer);
            remotingServer.start();

            RemotingClient remotingClient = new NettyClient();
            remotingClient.start();

            HashSet<String> set = new HashSet<>();
            set.add("topic1");
            set.add("topic2");

            RegisterBrokerBody master = new RegisterBrokerBody();
            master.setBrokerId(Constant.MASTER_ID);
            master.setBrokerName("broker1");
            master.setClusterName("cluster1");
            master.setBrokerAddress("172.18.45.13:" + port);
            master.setTopics(set);
            RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.REGISTER_BROKER, CommonCodec.encode(master));

            remotingClient.invokeOneway("localhost:9875", remotingCommand, 3000);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void registerRequestProcessor(RemotingServer remotingServer) {
        SendMessageProcessor sendMessageProcessor = new SendMessageProcessor();
        remotingServer.registerRequestProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor);
    }
}
