package bao.study.mymq.broker.store;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author baoyh
 * @since 2022/7/15 16:13
 */
public abstract class MessageStoreCodec {

    private static final Charset charset = StandardCharsets.UTF_8;

    public static ByteBuffer encode(MessageStore messageStore) {

        int size = 4 + 4 + 8 + 8 + 8;

        byte[] brokerName = messageStore.getBrokerName().getBytes(charset);
        byte[] topic = messageStore.getTopic().getBytes(charset);
        byte[] bornHost = messageStore.getBornHost().getAddress().getAddress();

        size = size + 4 + brokerName.length + 4 + topic.length + 4 + 4 + bornHost.length + 4 + messageStore.getBody().length;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(size);
        buffer.putInt(brokerName.length);
        buffer.put(brokerName);
        buffer.putInt(topic.length);
        buffer.put(topic);
        buffer.putInt(messageStore.getQueueId());
        buffer.putLong(messageStore.getCommitLogOffset());
        buffer.putInt(messageStore.getBody().length);
        buffer.put(messageStore.getBody());
        buffer.putInt(bornHost.length);
        buffer.putInt(messageStore.getBornHost().getPort());
        buffer.put(bornHost);
        buffer.putLong(messageStore.getBornTimeStamp());
        buffer.putLong(messageStore.getStoreTimeStamp());

        return buffer;
    }

    public static MessageStore decode(ByteBuffer buffer) {

        try {
            if (buffer.position() != 0) {
                buffer.position(0);
            }

            MessageStore messageStore = new MessageStore();

            messageStore.setSize(buffer.getInt());

            int brokerNameLen = buffer.getInt();
            byte[] brokerName = new byte[brokerNameLen];
            buffer.get(brokerName, 0, brokerNameLen);
            messageStore.setBrokerName(new String(brokerName, charset));

            int topicLen = buffer.getInt();
            byte[] topic = new byte[topicLen];
            buffer.get(topic, 0, topicLen);
            messageStore.setTopic(new String(topic, charset));

            messageStore.setQueueId(buffer.getInt());
            messageStore.setCommitLogOffset(buffer.getLong());

            int bodyLen = buffer.getInt();
            byte[] body = new byte[bodyLen];
            buffer.get(body, 0, bodyLen);
            messageStore.setBody(body);

            int hostLen = buffer.getInt();
            int port = buffer.getInt();
            byte[] bornHost = new byte[hostLen];
            buffer.get(bornHost, 0, hostLen);
            messageStore.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            messageStore.setBornTimeStamp(buffer.getLong());
            messageStore.setStoreTimeStamp(buffer.getLong());

            return messageStore;

        } catch (Exception e) {
            buffer.position(buffer.limit());
        }

        return null;
    }
}
