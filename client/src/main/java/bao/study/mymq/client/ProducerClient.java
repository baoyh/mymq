package bao.study.mymq.client;

import bao.study.mymq.client.producer.DefaultProducer;
import bao.study.mymq.client.producer.SendResult;
import bao.study.mymq.common.protocol.Message;

import java.nio.charset.StandardCharsets;

/**
 * @author baoyh
 * @since 2022/8/18 10:23
 */
public class ProducerClient {

    public static void main(String[] args) {
        DefaultProducer producer = new DefaultProducer();
        producer.setRouterAddress("localhost:9875");
        producer.start();

        int i = 0;
        while (i <= 5) {
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            System.out.println(result);
            i++;
        }
        producer.shutdown();
    }
}
