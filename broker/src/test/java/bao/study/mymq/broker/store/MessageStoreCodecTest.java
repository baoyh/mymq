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
public class MessageStoreCodecTest {

    @Test
    public void test() {
        MessageStore messageStore = new MessageStore();
        messageStore.setStoreTimeStamp(new Date().getTime());
        messageStore.setBornTimeStamp(new Date().getTime() - 10000);
        messageStore.setTopic("topic");
        messageStore.setBrokerName("中文名");
        messageStore.setBornHost(new InetSocketAddress(12345));
        messageStore.setSize(0);
        messageStore.setQueueId(1);
        messageStore.setCommitLogOffset(1000);
        messageStore.setBody(new byte[]{50, 51, 52});

        ByteBuffer buffer = MessageCodec.encode(messageStore);

        buffer.flip();

        print(Objects.requireNonNull(MessageCodec.decode(buffer)));

    }

    private void print(MessageStore messageStore) {
        System.out.println("Message{" +
                "size=" + messageStore.getSize() +
                ", brokerName='" + messageStore.getBrokerName() + '\'' +
                ", topic='" + messageStore.getTopic() + '\'' +
                ", queueId=" + messageStore.getQueueId() +
                ", commitLogOffset=" + messageStore.getCommitLogOffset() +
                ", bornHost=" + messageStore.getBornHost() +
                ", bornTimeStamp=" + messageStore.getBornTimeStamp() +
                ", storeTimeStamp=" + messageStore.getStoreTimeStamp() +
                ", body=" + Arrays.toString(messageStore.getBody()) +
                '}');
    }
}
