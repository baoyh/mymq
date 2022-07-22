package bao.study.mymq.broker.store;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

/**
 * @author baoyh
 * @since 2022/7/18 9:44
 */
public class MessageCodecTest {

    @Test
    public void test() {
        Message message = new Message();
        message.setStoreTimeStamp(new Date().getTime());
        message.setBornTimeStamp(new Date().getTime() - 10000);
        message.setTopic("topic");
        message.setBrokerName("中文名");
        message.setBornHost(new InetSocketAddress(12345));
        message.setSize(0);
        message.setQueueId(1);
        message.setCommitLogOffset(1000);
        message.setBody(new byte[]{50, 51, 52});

        ByteBuffer buffer = MessageCodec.encode(message);

        buffer.flip();

        print(Objects.requireNonNull(MessageCodec.decode(buffer)));

    }

    private void print(Message message) {
        System.out.println("Message{" +
                "size=" + message.getSize() +
                ", brokerName='" + message.getBrokerName() + '\'' +
                ", topic='" + message.getTopic() + '\'' +
                ", queueId=" + message.getQueueId() +
                ", commitLogOffset=" + message.getCommitLogOffset() +
                ", bornHost=" + message.getBornHost() +
                ", bornTimeStamp=" + message.getBornTimeStamp() +
                ", storeTimeStamp=" + message.getStoreTimeStamp() +
                ", body=" + Arrays.toString(message.getBody()) +
                '}');
    }
}
