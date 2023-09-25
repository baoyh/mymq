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
        while (i <= 20) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            SendResult result = producer.send(new Message("topic1", ("hello" + i).getBytes(StandardCharsets.UTF_8)));
            System.out.println(result);
            i++;
        }


//        producer.send(new Message("topic2", "world".getBytes(StandardCharsets.UTF_8)), new SendCallback() {
//
//            @Override
//            public void onSuccess(SendResult sendResult) {
//                System.out.println("send async success");
//                System.out.println(sendResult);
//            }
//
//            @Override
//            public void onException(Throwable e) {
//                System.out.println(Arrays.toString(e.getStackTrace()));
//            }
//        });

        producer.shutdown();
    }
}
