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
 * @since 2024/6/11 14:54
 */
public class BrokerTest {

    private static final Logger log = LoggerFactory.getLogger(BrokerTest.class);

    @Test
    public void testThreeServer() throws InterruptedException {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);

        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);

        // master
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        BrokerController a = CommonUtil.launchBroker(brokerProperties, topics);

        // slave1
        BrokerProperties brokerProperties2 = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10911, 1);
        BrokerController b = CommonUtil.launchBroker(brokerProperties2, topics);

        // slave2
        BrokerProperties brokerProperties3 = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10912, 2);
        BrokerController c = CommonUtil.launchBroker(brokerProperties3, topics);

        // 等待 broker 启动成功
        TimeUnit.SECONDS.sleep(2);

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
                count.getAndIncrement();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        // 等待消息消费成功发送通知给 broker
        TimeUnit.SECONDS.sleep(1);
        Assertions.assertEquals(count.get(), 10);

        producer.shutdown();
        consumer.shutdown();
        a.shutdown();
        b.shutdown();
        c.shutdown();
        router.shutdown();
    }

    @Test
    public void testThreeServerAndShutdownSlave() throws InterruptedException {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);

        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);

        // master
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        BrokerController a = CommonUtil.launchBroker(brokerProperties, topics);

        // slave1
        BrokerProperties brokerProperties2 = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10911, 1);
        BrokerController b = CommonUtil.launchBroker(brokerProperties2, topics);

        // slave2
        BrokerProperties brokerProperties3 = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10912, 2);
        BrokerController c = CommonUtil.launchBroker(brokerProperties3, topics);

        // 等待 broker 启动成功
        TimeUnit.SECONDS.sleep(2);

        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();
        for (int i = 0; i < 10; i++) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
        }

        // 关掉一个 slave, 可以正常消费
        b.shutdown();

        AtomicInteger count = new AtomicInteger();
        DefaultConsumer consumer = new DefaultConsumer();
        consumer.setRouterAddress("localhost:9875");
        consumer.subscribe("topic1");
        consumer.setGroup("test");
        consumer.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Success consume message: " + it.toString());
                count.getAndIncrement();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        // 等待消息消费成功发送通知给 broker
        TimeUnit.SECONDS.sleep(3);
        Assertions.assertEquals(count.get(), 10);

        producer.shutdown();
        consumer.shutdown();
        a.shutdown();
        c.shutdown();
        router.shutdown();
    }

    @Test
    public void testThreeServerAndShutdownMaster() throws InterruptedException {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);

        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);

        // master
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        BrokerController a = CommonUtil.launchBroker(brokerProperties, topics);

        // slave1
        BrokerProperties brokerProperties2 = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10911, 1);
        BrokerController b = CommonUtil.launchBroker(brokerProperties2, topics);

        // slave2
        BrokerProperties brokerProperties3 = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10912, 2);
        BrokerController c = CommonUtil.launchBroker(brokerProperties3, topics);

        // 等待 broker 启动成功
        TimeUnit.SECONDS.sleep(2);

        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();
        for (int i = 0; i < 10; i++) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
        }

        // 关闭 master
        a.shutdown();
        TimeUnit.SECONDS.sleep(2);

        AtomicInteger count = new AtomicInteger();
        DefaultConsumer consumer = new DefaultConsumer();
        consumer.setRouterAddress("localhost:9875");
        consumer.subscribe("topic1");
        consumer.setGroup("test");
        consumer.registerMessageListener(messages -> {
            messages.forEach(it -> {
                log.info("Success consume message: " + it.toString());
                count.getAndIncrement();
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        // 等待消息消费成功发送通知给 broker
        TimeUnit.SECONDS.sleep(4);
        Assertions.assertEquals(count.get(), 10);

        // producer 发送消息给新的 master
        for (int i = 0; i < 5; i++) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
        }

        producer.shutdown();
        consumer.shutdown();
        b.shutdown();
        c.shutdown();
        router.shutdown();
    }
}
