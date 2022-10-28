package bao.study.mymq.broker;


import bao.study.mymq.broker.manager.ConsumerOffsetManager;

/**
 * @author baoyh
 * @since 2022/10/14 10:28
 */
public class BrokerController {

    private final ConsumerOffsetManager consumerOffsetManager;

    public BrokerController(ConsumerOffsetManager consumerOffsetManager) {
        this.consumerOffsetManager = consumerOffsetManager;
    }

    public boolean initialize() {
        boolean result = true;
        result = result && consumerOffsetManager.load();
        return result;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }
}
