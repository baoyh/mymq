package bao.study.mymq.broker.config;

import bao.study.mymq.common.Constant;

import java.io.File;

/**
 * @author baoyh
 * @since 2023/6/7 17:42
 */
public class BrokerConfig {

    private String configRootPath;

    private static final String HOME_PATH = System.getProperty(Constant.MYMQ_HOME_PROPERTY, System.getenv(Constant.MYMQ_HOME_ENV));

    private static final String CONFIG_PATH = HOME_PATH + File.separator + "config";

    private static final String CONFIG_CONSUMEQUEUE_OFFSET = CONFIG_PATH + File.separator + "consumequeueOffset.json";

    private static final String CONFIG_COMMITLOG = CONFIG_PATH + File.separator + "commitlog.json";

    private static final String CONFIG_CONSUMEQUEUE = CONFIG_PATH + File.separator + "consumequeue.json";

    public String getConfigRootPath() {
        String homePath = System.getProperty(Constant.MYMQ_HOME_PROPERTY, System.getenv(Constant.MYMQ_HOME_ENV));
        configRootPath = homePath == null ? configRootPath : homePath + File.separator + "store";
        return configRootPath;
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

}
