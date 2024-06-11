package bao.study.mymq.test;

import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.broker.BrokerStartup;
import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.netty.NettyServer;
import bao.study.mymq.router.RouterStartup;
import bao.study.mymq.router.processor.RouterRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * @author baoyh
 * @since 2024/6/11 13:54
 */
public class CommonUtil {

    private static final Logger log = LoggerFactory.getLogger(CommonUtil.class);

    public static void launchRouter(int port) {
        RemotingServer remotingServer = new NettyServer(port);
        try {
            remotingServer.registerRequestProcessor(new RouterRequestProcessor(), RequestCode.REGISTER_BROKER, RequestCode.GET_ROUTE_BY_TOPIC, RequestCode.QUERY_BROKERS_BY_BROKER_NAME);
            remotingServer.start();
            log.info("router started");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void launchBroker(BrokerProperties brokerProperties, Map<String, Integer> topics) {
        BrokerStartup.setBrokerProperties(brokerProperties);
        BrokerStartup.start(brokerProperties.getPort(), topics);
        log.info("broker started");
    }

    public static void clear() {
        try {
            String path = System.getProperty("user.dir");
            Files.deleteIfExists(Paths.get(path + File.separator + "store"));
            Files.deleteIfExists(Paths.get(path + File.separator + "config"));
        } catch (Exception e) {
            log.error("clear failed", e);
        }
    }

}
