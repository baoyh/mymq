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
import java.util.HashSet;

/**
 * @author baoyh
 * @since 2022/5/24 11:24
 */
public class BrokerStartup {

    public static void main(String[] args) {

        try {
            RemotingServer remotingServer = new NettyServer(10910);
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
            master.setBrokerAddress("172.18.1.1:8080");
            master.setTopics(set);
            RemotingCommand remotingCommand = new RemotingCommand();
            remotingCommand.setCode(RequestCode.REGISTER_BROKER);
            remotingCommand.setBody(CommonCodec.encode(master));

            RegisterBrokerBody slave = new RegisterBrokerBody();
            slave.setBrokerId(1);
            slave.setBrokerName("broker1");
            slave.setClusterName("cluster1");
            slave.setBrokerAddress("172.18.1.1:8081");
            slave.setTopics(set);
            RemotingCommand remotingCommand2 = new RemotingCommand();
            remotingCommand2.setCode(RequestCode.REGISTER_BROKER);
            remotingCommand2.setBody(CommonCodec.encode(slave));

            RegisterBrokerBody broker2 = new RegisterBrokerBody();
            broker2.setBrokerId(Constant.MASTER_ID);
            broker2.setBrokerName("broker2");
            broker2.setClusterName("cluster1");
            broker2.setBrokerAddress("172.18.1.1:8090");
            broker2.setTopics(set);
            RemotingCommand remotingCommand3 = new RemotingCommand();
            remotingCommand3.setCode(RequestCode.REGISTER_BROKER);
            remotingCommand3.setBody(CommonCodec.encode(broker2));

            remotingClient.invokeOneway("localhost:9875", remotingCommand, 3000);
            remotingClient.invokeOneway("localhost:9875", remotingCommand2, 3000);
            remotingClient.invokeOneway("localhost:9875", remotingCommand3, 3000);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
