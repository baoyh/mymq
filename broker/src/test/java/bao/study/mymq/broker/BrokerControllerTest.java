package bao.study.mymq.broker;

import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.body.RegisterBrokerBody;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.netty.NettyClient;
import bao.study.mymq.remoting.netty.NettyServer;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author baoyh
 * @since 2022/7/20 14:42
 */
public class BrokerControllerTest {

    RemotingClient remotingClient = new NettyClient();
    RemotingServer remotingServer = new NettyServer(1234);

    @Before
    public void before() {
        remotingClient.start();
        remotingServer.start();
    }

    @Test
    public void registerBroker() {
        RegisterBrokerBody master = new RegisterBrokerBody();
        master.setBrokerId(Constant.MASTER_ID);
        master.setBrokerName("broker1");
        master.setClusterName("cluster1");
        master.setBrokerAddress("172.18.1.1:8080");
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);
        topics.put("topic2", 4);
        master.setTopics(topics);
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setCode(RequestCode.REGISTER_BROKER);
        remotingCommand.setBody(CommonCodec.encode(master));

        RegisterBrokerBody slave = new RegisterBrokerBody();
        slave.setBrokerId(1);
        slave.setBrokerName("broker1");
        slave.setClusterName("cluster1");
        slave.setBrokerAddress("172.18.1.1:8081");
        slave.setTopics(topics);
        RemotingCommand remotingCommand2 = new RemotingCommand();
        remotingCommand2.setCode(RequestCode.REGISTER_BROKER);
        remotingCommand2.setBody(CommonCodec.encode(slave));

        RegisterBrokerBody broker2 = new RegisterBrokerBody();
        broker2.setBrokerId(Constant.MASTER_ID);
        broker2.setBrokerName("broker2");
        broker2.setClusterName("cluster1");
        broker2.setBrokerAddress("172.18.1.1:8090");
        broker2.setTopics(topics);
        RemotingCommand remotingCommand3 = new RemotingCommand();
        remotingCommand3.setCode(RequestCode.REGISTER_BROKER);
        remotingCommand3.setBody(CommonCodec.encode(broker2));

        remotingClient.invokeOneway("localhost:9875", remotingCommand, 3000);
        remotingClient.invokeOneway("localhost:9875", remotingCommand2, 3000);
        remotingClient.invokeOneway("localhost:9875", remotingCommand3, 3000);
    }
}
