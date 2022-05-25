package bao.study.mymq.store;

import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.netty.NettyClient;
import bao.study.mymq.remoting.netty.NettyServer;

/**
 * @author baoyh
 * @since 2022/5/24 11:24
 */
public class StoreStartup {

    public static void main(String[] args) {

        try {
            RemotingServer remotingServer = new NettyServer(10910);
            remotingServer.start();

            RemotingClient remotingClient = new NettyClient();
            remotingClient.start();

            RemotingCommand remotingCommand = new RemotingCommand();
            remotingCommand.setCode(RequestCode.REGISTER_STORE);
            remotingCommand.setBody(new byte[]{1, 2, 3, 4, 5});

            remotingClient.invokeOneway("localhost:9875", remotingCommand, 3000);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
