package bao.study.mymq.test;

import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.client.consumer.ConsumeConcurrentlyStatus;
import bao.study.mymq.client.consumer.DefaultConsumer;
import bao.study.mymq.client.producer.DefaultProducer;
import bao.study.mymq.client.producer.SendResult;
import bao.study.mymq.client.producer.SendStatus;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author baoyh
 * @since 2024/6/11 14:41
 */
public class ConsumeMessageTest {

    private static final Logger log = LoggerFactory.getLogger(ConsumeMessageTest.class);

    @Test
    public void testConsumeMessage() throws InterruptedException {
        CommonUtil.clear();
        CommonUtil.launchRouter(9875);
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);
        CommonUtil.launchBroker(brokerProperties, topics);

        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();
        for (int i = 0; i < 10; i++) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
        }

        DefaultConsumer consumer = new DefaultConsumer();
        consumer.setRouterAddress("localhost:9875");
        consumer.subscribe("topic1");
        consumer.setGroup("test");
        consumer.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Success consume message: " + it.toString());
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        // 等待消息消费成功发送通知给 broker
        TimeUnit.SECONDS.sleep(1);
    }
}
