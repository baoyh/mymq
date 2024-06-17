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
            RouterRequestProcessor processor = new RouterRequestProcessor();
            registerRequestProcessor(remotingServer, processor);
            remotingServer.start();
            processor.start();
            log.info("router started");
        } catch (Throwable e) {
            log.error("router started failed", e);
            System.exit(-1);
        }
    }

    private static void registerRequestProcessor(RemotingServer remotingServer, RouterRequestProcessor processor) {
        remotingServer.registerRequestProcessor(processor, RequestCode.REGISTER_BROKER, RequestCode.GET_ROUTE_BY_TOPIC, RequestCode.REGISTER_MASTER,
                RequestCode.REGISTER_CONSUMER, RequestCode.QUERY_CONSUMERS_BY_GROUP, RequestCode.BROKER_HEARTBEAT, RequestCode.QUERY_ALIVE_BROKERS);
    }
}
