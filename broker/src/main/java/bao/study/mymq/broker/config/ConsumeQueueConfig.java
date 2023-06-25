package bao.study.mymq.broker.config;

import java.io.File;

/**
 * @author baoyh
 * @since 2023/6/7 17:39
 */
public class ConsumeQueueConfig extends BrokerConfig {

    private String consumeQueuePath;

    private final int size = 12 * 1024;

    public String getConsumeQueuePath() {
        if (consumeQueuePath == null) {
            consumeQueuePath = getConfigRootPath() + File.separator + "consumequeue";
        }
        return consumeQueuePath;
    }

    public String firstName() {
        return "00000000";
    }

    public int getSize() {
        return size;
    }
}
