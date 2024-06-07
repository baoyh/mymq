package bao.study.mymq.router;

import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.netty.NettyServer;
import bao.study.mymq.router.processor.RouterRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author baoyh
 * @since 2022/5/13 14:38
 */
public class RouterStartup {

    private static final Logger log = LoggerFactory.getLogger(RouterStartup.class);

    public static void main(String[] args) {
        RemotingServer remotingServer = new NettyServer(9875);
        try {
            registerRequestProcessor(remotingServer);
            remotingServer.start();
            log.info("router started");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void registerRequestProcessor(RemotingServer remotingServer) {
        remotingServer.registerRequestProcessor(new RouterRequestProcessor(), RequestCode.REGISTER_BROKER, RequestCode.GET_ROUTE_BY_TOPIC, RequestCode.QUERY_BROKERS_BY_BROKER_NAME);
    }
}
