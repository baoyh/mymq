package bao.study.mymq.test;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.broker.BrokerStartup;
import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.netty.NettyServer;
import bao.study.mymq.router.processor.RouterRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * @author baoyh
 * @since 2024/6/11 13:54
 */
public class CommonUtil {

    private static final Logger log = LoggerFactory.getLogger(CommonUtil.class);

    public static RemotingServer launchRouter(int port) {
        RemotingServer remotingServer = new NettyServer(port);
        try {
            RouterRequestProcessor processor = new RouterRequestProcessor();
            remotingServer.registerRequestProcessor(new RouterRequestProcessor(), RequestCode.REGISTER_BROKER,
                    RequestCode.GET_ROUTE_BY_TOPIC, RequestCode.BROKER_HEARTBEAT, RequestCode.QUERY_ALIVE_BROKERS);
            remotingServer.start();
            processor.start();
            log.info("router started");
        } catch (Throwable e) {
            log.error("router started failed", e);
            System.exit(-1);
        }
        return remotingServer;
    }


    public static BrokerController launchBroker(BrokerProperties brokerProperties, Map<String, Integer> topics) {
        BrokerStartup.setBrokerProperties(brokerProperties);
        BrokerController controller = BrokerStartup.start(brokerProperties.getPort(), topics);
        log.info("broker started");
        return controller;
    }

    public static void clear() {
        try {
            String path = System.getProperty("user.dir");
            deleteDir(path + File.separator + "store");
            deleteDir(path + File.separator + "config");
        } catch (Exception e) {
            log.error("clear failed", e);
        }
    }

    public static void deleteDir(String dirPath) {
        File file = new File(dirPath);
        if (!file.isFile()) {
            File[] files = file.listFiles();
            if (files == null) {
                file.delete();
            } else {
                for (File f : files) {
                    deleteDir(f.getAbsolutePath());
                }
            }
        }
        file.delete();
    }


}
