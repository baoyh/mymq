package bao.study.mymq.test;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.client.producer.DefaultProducer;
import bao.study.mymq.client.producer.SendCallback;
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

/**
 * @author baoyh
 * @since 2024/6/11 14:08
 */
public class SendMessageTest {

    private static final Logger log = LoggerFactory.getLogger(SendMessageTest.class);

    @Test
    public void testSendMessage() {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);
        BrokerController broker = CommonUtil.launchBroker(brokerProperties, topics);

        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();

        int queueId = 0;
        for (int i = 0; i < 10; i++) {
            if (queueId == 4) {
                queueId = 0;
            }
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
            Assertions.assertEquals(result.getMessageQueue().getQueueId(), queueId);
            queueId++;
        }

        producer.shutdown();
        broker.shutdown();
        router.shutdown();
    }

    @Test
    public void testSendMessageAsync() throws InterruptedException {
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
            producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)), new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("send success: {}", sendResult);
                    Assertions.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
                }

                @Override
                public void onException(Throwable e) {

                }
            });
        }

        // 等待消息发送完成并回调
        TimeUnit.SECONDS.sleep(1);

        producer.shutdown();
        broker.shutdown();
        router.shutdown();
    }

    @Test
    public void testMultiProducer() throws InterruptedException {
        CommonUtil.clear();
        RemotingServer router = CommonUtil.launchRouter(9875);
        BrokerProperties brokerProperties = new BrokerProperties("broker1", "cluster1", "localhost:9875", 10910, Constant.MASTER_ID);
        Map<String, Integer> topics = new HashMap<>();
        topics.put("topic1", 4);
        topics.put("topic2", 4);
        BrokerController broker = CommonUtil.launchBroker(brokerProperties, topics);

        DefaultProducer a = new DefaultProducer();
        a.setRouterAddress("localhost:9875");
        a.start();

        DefaultProducer b = new DefaultProducer();
        b.setRouterAddress("localhost:9875");
        b.start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                SendResult result = a.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
                Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                SendResult result = b.send(new Message("topic2", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
                Assertions.assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
            }
        }).start();

        // 等待消息发送完成并回调
        TimeUnit.SECONDS.sleep(1);

        a.shutdown();
        b.shutdown();
        broker.shutdown();
        router.shutdown();
    }
}
