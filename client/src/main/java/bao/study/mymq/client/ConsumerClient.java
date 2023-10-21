package bao.study.mymq.client;

import bao.study.mymq.client.consumer.ConsumeConcurrentlyStatus;
import bao.study.mymq.client.consumer.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author baoyh
 * @since 2023/5/5 20:26
 */
public class ConsumerClient {

    private static final Logger log = LoggerFactory.getLogger(DefaultConsumer.class);

    public static void main(String[] args) {
        DefaultConsumer consumer = new DefaultConsumer();
        consumer.setRouterAddress("localhost:9875");
        consumer.subscribe("topic1");
        consumer.setGroup("test");
        consumer.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info(new String(it.getBody()));
                log.info(it.toString());
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }
}
