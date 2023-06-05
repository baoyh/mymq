package bao.study.mymq.broker.util;

import bao.study.mymq.common.Constant;

import java.io.File;

/**
 * @author baoyh
 * @since 2022/10/26 11:10
 */
public abstract class BrokerConfig {

    private static final String HOME_PATH = System.getProperty(Constant.MYMQ_HOME_PROPERTY, System.getenv(Constant.MYMQ_HOME_ENV));

    private static final String CONFIG_PATH = HOME_PATH + File.separator + "config";

    public static String consumerOffsetConfigPath() {
        return CONFIG_PATH + File.separator + "consumerOffset.json";
    }

    public static String consumeQueuePath() {
        return CONFIG_PATH + File.separator + "consumequeue";
    }

}
