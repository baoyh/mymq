package bao.study.mymq.client.consumer;

import bao.study.mymq.client.Client;

/**
 * @author baoyh
 * @since 2022/10/27 10:39
 */
public class DefaultConsumer extends Client implements Consumer {

    private String topic;

    private MessageListener messageListener;

    private PullMessageService pullMessageService = new PullMessageService();

    @Override
    protected void doStart() {
        pullMessageService.start();
    }

    @Override
    protected void doShutdown() {

    }

    @Override
    public void subscribe(String topic) {
        assert topic != null;
        this.topic = topic;
    }

    @Override
    public void registerMessageListener(MessageListener messageListener) {
        assert messageListener != null;
        this.messageListener = messageListener;
    }
}
