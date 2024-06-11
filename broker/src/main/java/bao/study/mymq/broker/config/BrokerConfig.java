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

    private static final String CONFIG_CONSUMEQUEUE_OFFSET = CONFIG_PATH + File.separator + "consumequeueIndex.json";

    private static final String CONFIG_COMMITLOG = CONFIG_PATH + File.separator + "commitlog.json";

    private static final String CONFIG_CONSUMEQUEUE = CONFIG_PATH + File.separator + "consumequeue.json";

    private static final int holdTime = 20 * 1000;

    private static final long rpcTimeoutMillis = 3000;

    public String getConfigRootPath() {
        return CONFIG_PATH;
    }

    public static String consumeQueueOffsetConfigPath() {
        return CONFIG_CONSUMEQUEUE_OFFSET;
    }

    public static String commitlogConfigPath() {
        return CONFIG_COMMITLOG;
    }

    public static String consumeQueueConfigPath() {
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
