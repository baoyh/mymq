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
public abstract class MessageCodec {

    private static final Charset charset = StandardCharsets.UTF_8;

    public static ByteBuffer encode(Message message) {

        int size = 4 + 4 + 8 + 8 + 8;

        byte[] brokerName = message.getBrokerName().getBytes(charset);
        byte[] topic = message.getTopic().getBytes(charset);
        byte[] bornHost = message.getBornHost().getAddress().getAddress();

        size = size + 4 + brokerName.length + 4 + topic.length + 4 + 4 + bornHost.length + 4 + message.getBody().length;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(size);
        buffer.putInt(brokerName.length);
        buffer.put(brokerName);
        buffer.putInt(topic.length);
        buffer.put(topic);
        buffer.putInt(message.getQueueId());
        buffer.putLong(message.getCommitLogOffset());
        buffer.putInt(message.getBody().length);
        buffer.put(message.getBody());
        buffer.putInt(bornHost.length);
        buffer.putInt(message.getBornHost().getPort());
        buffer.put(bornHost);
        buffer.putLong(message.getBornTimeStamp());
        buffer.putLong(message.getStoreTimeStamp());

        return buffer;
    }

    public static Message decode(ByteBuffer buffer) {

        try {
            if (buffer.position() != 0) {
                buffer.position(0);
            }

            Message message = new Message();

            message.setSize(buffer.getInt());

            int brokerNameLen = buffer.getInt();
            byte[] brokerName = new byte[brokerNameLen];
            buffer.get(brokerName, 0, brokerNameLen);
            message.setBrokerName(new String(brokerName, charset));

            int topicLen = buffer.getInt();
            byte[] topic = new byte[topicLen];
            buffer.get(topic, 0, topicLen);
            message.setTopic(new String(topic, charset));

            message.setQueueId(buffer.getInt());
            message.setCommitLogOffset(buffer.getLong());

            int bodyLen = buffer.getInt();
            byte[] body = new byte[bodyLen];
            buffer.get(body, 0, bodyLen);
            message.setBody(body);

            int hostLen = buffer.getInt();
            int port = buffer.getInt();
            byte[] bornHost = new byte[hostLen];
            buffer.get(bornHost, 0, hostLen);
            message.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            message.setBornTimeStamp(buffer.getLong());
            message.setStoreTimeStamp(buffer.getLong());

            return message;

        } catch (Exception e) {
            buffer.position(buffer.limit());
        }

        return null;
    }
}
