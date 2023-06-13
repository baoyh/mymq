package bao.study.mymq.client;

import bao.study.mymq.client.consumer.DefaultConsumer;

/**
 * @author baoyh
 * @since 2023/5/5 20:26
 */
public class ConsumerClient {

    public static void main(String[] args) {
        DefaultConsumer consumer = new DefaultConsumer();
        consumer.setRouterAddress("localhost:9875");
        consumer.subscribe("topic1");
        consumer.setGroup("test");
        consumer.registerMessageListener((messages) -> messages.forEach(it -> {
            System.out.println(new String(it.getBody()));
            System.out.println(messages);
        }));
        consumer.start();
    }
}
