package bao.study.mymq.broker.config;


import java.io.File;

/**
 * @author baoyh
 * @since 2023/6/7 17:42
 */
public class BrokerConfig {

//    private static final String HOME_PATH = System.getProperty("user.home") + File.separator + "mymq";

    private static final String HOME_PATH = System.getProperty("user.dir");

    private static final String DATA_PATH = HOME_PATH + File.separator + "store";

    private static final String CONFIG_PATH = HOME_PATH + File.separator + "config";

    private static final String CONFIG_CONSUMEQUEUE_OFFSET = "consumequeueIndex.json";

    private static final String CONFIG_COMMITLOG = "commitlog.json";

    private static final String CONFIG_CONSUMEQUEUE = "consumequeue.json";

    private static final int holdTime = 20 * 1000;

    private static final long rpcTimeoutMillis = 3000;

    public static String getConfigRootPath() {
        return CONFIG_PATH;
    }

    public static String consumeQueueOffsetConfigName() {
        return CONFIG_CONSUMEQUEUE_OFFSET;
    }

    public static String commitlogConfigName() {
        return CONFIG_COMMITLOG;
    }

    public static String consumeQueueConfigName() {
        return CONFIG_CONSUMEQUEUE;
    }

    public static String dataPath() {
        return DATA_PATH;
    }

    public static int getHoldTime() {
        return holdTime;
    }

    public static long getRpcTimeoutMillis() {
        return rpcTimeoutMillis;
    }

}
