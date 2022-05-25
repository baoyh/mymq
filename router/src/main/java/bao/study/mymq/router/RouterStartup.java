package bao.study.mymq.router;

import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.netty.NettyServer;
import bao.study.mymq.router.processor.RouterRequestProcessor;

/**
 * @author baoyh
 * @since 2022/5/13 14:38
 */
public class RouterStartup {

    public static void main(String[] args) {
        RemotingServer remotingServer = new NettyServer(9875);
        try {
            registerRequestProcessor(remotingServer);
            remotingServer.start();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void registerRequestProcessor(RemotingServer remotingServer) {
        RouterRequestProcessor routerRequestProcessor = new RouterRequestProcessor();
        remotingServer.registerRequestProcessor(RequestCode.REGISTER_STORE, routerRequestProcessor);
    }
}
