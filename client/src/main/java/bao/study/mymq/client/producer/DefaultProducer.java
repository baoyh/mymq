package bao.study.mymq.client.producer;

import bao.study.mymq.client.BrokerInstance;
import bao.study.mymq.client.Client;
import bao.study.mymq.client.ClientException;
import bao.study.mymq.common.protocol.Message;
import bao.study.mymq.common.protocol.MessageExt;
import bao.study.mymq.common.protocol.TopicPublishInfo;
import bao.study.mymq.common.protocol.broker.BrokerData;
import bao.study.mymq.common.protocol.message.MessageQueue;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingMode;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author baoyh
 * @since 2022/8/2 14:05
 */
public class DefaultProducer extends Client implements Producer {

    private static final Logger log = LoggerFactory.getLogger(DefaultProducer.class);

    private final BrokerInstance brokerInstance = new BrokerInstance();

    private final ThreadLocal<Map<String /* topic */, Integer /* count */>> sendWhichQueue = ThreadLocal.withInitial(HashMap::new);

    private long sendMessageTimeOut = 300 * 1000;

    private int sendMessageRetryTimes = 3;

    @Override
    protected void doStart() {
    }

    @Override
    public void doShutdown() {
    }

    @Override
    public SendResult send(Message message) {
        return sendImpl(message, RemotingMode.SYNC, null);
    }

    @Override
    public void send(Message message, SendCallback sendCallback) {
        sendImpl(message, RemotingMode.ASYNC, sendCallback);
    }

    @Override
    public void sendOneway(Message message) {

    }

    private SendResult sendImpl(Message message, RemotingMode remotingMode, SendCallback sendCallback) {

        SendResult sendResult = new SendResult();

        String topic = message.getTopic();
        TopicPublishInfo topicPublishInfo = brokerInstance.findTopicPublishInfo(topic, this);
        List<MessageQueue> messageQueueList = topicPublishInfo.getMessageQueueList();

        String lastFailedBrokerName = null;

        for (int i = 0; i < sendMessageRetryTimes; i++) {
            MessageQueue messageQueue = this.selectOneMessageQueue(topic, messageQueueList, lastFailedBrokerName);

            try {
                MessageExt messageExt = createMessageExt(message, messageQueue);
                RemotingCommand request = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.SEND_MESSAGE, CommonCodec.encode(messageExt));

                String brokerAddress = findBrokerAddress(messageQueue.getBrokerName(), topicPublishInfo);

                switch (remotingMode) {
                    case SYNC:
                        RemotingCommand remotingCommand = remotingClient.invokeSync(brokerAddress, request, sendMessageTimeOut);
                        if (remotingCommand.getCode() == ResponseCode.SUCCESS) {
                            sendResult.setSendStatus(SendStatus.SEND_OK);
                            sendResult.setMessageQueue(messageQueue);
                        }
                        break;
                    case ASYNC:
                        remotingClient.invokeAsync(brokerAddress, request, sendMessageTimeOut, (responseFuture) -> {

                            if (responseFuture.getException() == null) {
                                sendResult.setSendStatus(SendStatus.SEND_OK);
                                sendResult.setMessageQueue(messageQueue);
                                sendCallback.onSuccess(sendResult);
                            } else {
                                sendCallback.onException(responseFuture.getException());
                            }
                        });
                        break;
                    case ONEWAY:
                        remotingClient.invokeOneway(brokerAddress, request, sendMessageTimeOut);
                        break;
                }

                break;

            } catch (Exception e) {
                log.error("Connect to broker " + messageQueue.getBrokerName() + " fail", e);
                lastFailedBrokerName = messageQueue.getBrokerName();
                topicPublishInfo = brokerInstance.updateTopicPublishInfo(topic, this);
            }

        }

        return sendResult;
    }

    private MessageExt createMessageExt(Message message, MessageQueue messageQueue) {
        MessageExt messageExt = new MessageExt();
        messageExt.setBody(message.getBody());
        messageExt.setTopic(message.getTopic());
        messageExt.setQueueId(messageQueue.getQueueId());
        messageExt.setBrokerName(messageQueue.getBrokerName());
        messageExt.setBornTimeStamp(System.currentTimeMillis());
        return messageExt;
    }

    private MessageQueue selectOneMessageQueue(String topic, List<MessageQueue> messageQueueList, String lastFailedBrokerName) {

        Map<String, Integer> topicMap = sendWhichQueue.get();
        if (!topicMap.containsKey(topic)) {
            topicMap.put(topic, 0);
        }

        if (lastFailedBrokerName == null) {
            return selectOneMessageQueue(topic, messageQueueList);
        }

        List<MessageQueue> copyMessageQueueList = new ArrayList<>();
        for (MessageQueue messageQueue : messageQueueList) {
            if (!messageQueue.getBrokerName().equals(lastFailedBrokerName)) {
                copyMessageQueueList.add(messageQueue);
            }
        }
        // 处理只有一个 broker 的情况
        if (copyMessageQueueList.isEmpty()) {
            copyMessageQueueList = messageQueueList;
        }

        return selectOneMessageQueue(topic, copyMessageQueueList);
    }

    private MessageQueue selectOneMessageQueue(String topic, List<MessageQueue> messageQueueList) {
        Integer index = sendWhichQueue.get().get(topic);
        MessageQueue messageQueue = messageQueueList.get(index % messageQueueList.size());
        sendWhichQueue.get().put(topic, ++index);
        return messageQueue;
    }

    private String findBrokerAddress(String brokerName, TopicPublishInfo topicPublishInfo) {

        List<BrokerData> brokerDataList = topicPublishInfo.getBrokerDataList();
        for (BrokerData brokerData : brokerDataList) {
            if (brokerData.getBrokerName().equals(brokerName)) {
                return brokerData.getAddressMap().get(brokerData.getMasterId());
            }
        }

        throw new ClientException("Cannot find the broker [" + brokerName + "] address");
    }

    public void setSendMessageTimeOut(long sendMessageTimeOut) {
        this.sendMessageTimeOut = sendMessageTimeOut;
    }

    public void setSendMessageRetryTimes(int sendMessageRetryTimes) {
        this.sendMessageRetryTimes = sendMessageRetryTimes < 1 ? 3 : sendMessageRetryTimes;
    }
}
