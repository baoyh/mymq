package bao.study.mymq.test;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.client.consumer.ConsumeConcurrentlyStatus;
import bao.study.mymq.client.consumer.DefaultConsumer;
import bao.study.mymq.client.producer.DefaultProducer;
import bao.study.mymq.client.producer.SendResult;
import bao.study.mymq.client.producer.SendStatus;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.Message;
import bao.study.mymq.remoting.RemotingServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author baoyh
 * @since 2024/6/11 14:41
 */
public class ConsumeMessageTest {

    private static final Logger log = LoggerFactory.getLogger(ConsumeMessageTest.class);

    @Test
    public void testConsumeMessage() throws InterruptedException {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);
        BrokerController broker = CommonUtil.launchBroker(brokerProperties, topics);

        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();
        for (int i = 0; i < 10; i++) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
        }

        AtomicInteger count = new AtomicInteger();
        DefaultConsumer consumer = new DefaultConsumer();
        consumer.setRouterAddress("localhost:9875");
        consumer.subscribe("topic1");
        consumer.setGroup("test");
        consumer.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Success consume message: " + it.toString());
                count.incrementAndGet();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        // 等待消息消费成功发送通知给 broker
        TimeUnit.SECONDS.sleep(1);
        Assertions.assertEquals(count.get(), 10);

        consumer.shutdown();
        producer.shutdown();
        broker.shutdown();
        router.shutdown();
    }

    @Test
    public void testMultiTopic() throws InterruptedException {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);
        topics.put("topic2", 4);
        BrokerController broker = CommonUtil.launchBroker(brokerProperties, topics);

        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();
        for (int i = 0; i < 10; i++) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
            result = producer.send(new Message("topic2", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
        }

        AtomicInteger count = new AtomicInteger();
        DefaultConsumer consumer = new DefaultConsumer();
        consumer.setRouterAddress("localhost:9875");
        consumer.subscribe("topic1");
        consumer.subscribe("topic2");
        consumer.setGroup("test");
        consumer.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Success consume message {}, topic {}" , it.toString(), it.getTopic());
                count.incrementAndGet();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        // 等待消息消费成功发送通知给 broker
        TimeUnit.SECONDS.sleep(1);
        Assertions.assertEquals(count.get(), 20);

        consumer.shutdown();
        producer.shutdown();
        broker.shutdown();
        router.shutdown();
    }

    @Test
    public void testMultiConsume() throws InterruptedException {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);
        BrokerController broker = CommonUtil.launchBroker(brokerProperties, topics);

        AtomicInteger count = new AtomicInteger();
        DefaultConsumer a = new DefaultConsumer();
        a.setRouterAddress("localhost:9875");
        a.subscribe("topic1");
        a.setGroup("test");
        a.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Consumer a success consume message: " + it.toString());
                count.incrementAndGet();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        DefaultConsumer b = new DefaultConsumer();
        b.setRouterAddress("localhost:9875");
        b.subscribe("topic1");
        b.setGroup("test");
        b.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Consumer b success consume message: " + it.toString());
                count.incrementAndGet();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        new Thread(a::start).start();
        new Thread(b::start).start();
        // 等待 consumer 完成同步, 系统无法完全保证消费不重复, 可适当延长等待时间
        TimeUnit.SECONDS.sleep(3);

        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();
        for (int i = 0; i < 10; i++) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
        }

        // 等待消息消费成功发送通知给 broker
        TimeUnit.SECONDS.sleep(2);
        Assertions.assertEquals(count.get(), 10);

        a.shutdown();
        b.shutdown();
        producer.shutdown();
        broker.shutdown();
        router.shutdown();
    }

    @Test
    public void testMultiConsumeDifferentGroup() throws InterruptedException {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);
        BrokerController broker = CommonUtil.launchBroker(brokerProperties, topics);

        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();
        for (int i = 0; i < 10; i++) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
        }

        AtomicInteger count = new AtomicInteger();
        DefaultConsumer a = new DefaultConsumer();
        a.setRouterAddress("localhost:9875");
        a.subscribe("topic1");
        a.setGroup("test");
        a.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Consumer a success consume message: " + it.toString());
                count.incrementAndGet();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        DefaultConsumer b = new DefaultConsumer();
        b.setRouterAddress("localhost:9875");
        b.subscribe("topic1");
        b.setGroup("test2");
        b.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Consumer b success consume message: " + it.toString());
                count.incrementAndGet();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        new Thread(a::start).start();
        new Thread(b::start).start();

        // 等待消息消费成功发送通知给 broker
        TimeUnit.SECONDS.sleep(1);
        Assertions.assertEquals(count.get(), 20);

        a.shutdown();
        b.shutdown();
        producer.shutdown();
        broker.shutdown();
        router.shutdown();
    }
}
