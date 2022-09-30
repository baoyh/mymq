package bao.study.mymq.client.producer;

import bao.study.mymq.client.ClientConfig;
import bao.study.mymq.client.ClientException;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.ServiceState;
import bao.study.mymq.common.protocol.Message;
import bao.study.mymq.common.protocol.MessageExt;
import bao.study.mymq.common.protocol.TopicPublishInfo;
import bao.study.mymq.common.protocol.broker.BrokerData;
import bao.study.mymq.common.protocol.message.MessageQueue;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingMode;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyClient;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author baoyh
 * @since 2022/8/2 14:05
 */
public class DefaultProducer extends ClientConfig implements Producer {

    private final RemotingClient remotingClient = new NettyClient();

    private final Set<DefaultProducer> producerSet = new CopyOnWriteArraySet<>();

    private final Map<String /* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();

    private final Map<String /* brokerName */, String /* address */> brokerAddressTable = new ConcurrentHashMap<>();

    private final ThreadLocal<Map<String /* topic */, Integer /* count */>> sendWhichQueue = ThreadLocal.withInitial(HashMap::new);

    private ServiceState serviceState = ServiceState.JUST_START;

    private long sendMessageTimeOut = 300 * 1000;

    private int sendMessageRetryTimes = 3;

    @Override
    public void start() {
        switch (serviceState) {
            case JUST_START:
                doStart();
                break;
            case START_FAIL:
                throw new ClientException("client start fail");
            default:
                break;
        }

    }

    private void doStart() {
        try {
            remotingClient.start();
            producerSet.add(this);
            serviceState = ServiceState.RUNNING;

        } catch (Exception e) {
            serviceState = ServiceState.START_FAIL;
            throw new ClientException("client start fail", e);
        }

    }

    @Override
    public void shutdown() {
        serviceState = ServiceState.SHUTDOWN;
        producerSet.remove(this);
        remotingClient.shutdown();
    }

    @Override
    public SendResult send(Message message) {
        return doSend(message, RemotingMode.SYNC, null);
    }

    @Override
    public void send(Message message, SendCallback sendCallback) {
        doSend(message, RemotingMode.ASYNC, sendCallback);
    }

    @Override
    public void sendOneway(Message message) {

    }

    private SendResult doSend(Message message, RemotingMode remotingMode, SendCallback sendCallback) {

        SendResult sendResult = new SendResult();

        String topic = message.getTopic();
        TopicPublishInfo topicPublishInfo = findTopicPublishInfo(topic);
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
                lastFailedBrokerName = messageQueue.getBrokerName();
            }

        }

        return sendResult;
    }

    private MessageExt createMessageExt(Message message, MessageQueue messageQueue) {
        MessageExt messageExt = new MessageExt();
        messageExt.setBody(message.getBody());
        messageExt.setTopic(message.getTopic());
        messageExt.setBrokerName(messageQueue.getBrokerName());
        messageExt.setBornTimeStamp(System.currentTimeMillis());
        return messageExt;
    }

    private TopicPublishInfo findTopicPublishInfo(String topic) {

        if (topicPublishInfoTable.containsKey(topic)) {
            return topicPublishInfoTable.get(topic);
        }

        RemotingCommand request = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.GET_ROUTE_BY_TOPIC, CommonCodec.encode(topic));
        RemotingCommand response = remotingClient.invokeSync(this.getRouterAddress(), request, sendMessageTimeOut);

        TopicPublishInfo topicPublishInfo = CommonCodec.decode(response.getBody(), TopicPublishInfo.class);
        topicPublishInfoTable.put(topic, topicPublishInfo);
        return topicPublishInfo;
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
        return selectOneMessageQueue(topic, copyMessageQueueList);
    }

    private MessageQueue selectOneMessageQueue(String topic, List<MessageQueue> messageQueueList) {
        Integer index = sendWhichQueue.get().get(topic);
        MessageQueue messageQueue = messageQueueList.get(index % messageQueueList.size());
        sendWhichQueue.get().put(topic, ++index);
        return messageQueue;
    }

    private String findBrokerAddress(String brokerName, TopicPublishInfo topicPublishInfo) {
        if (brokerAddressTable.containsKey(brokerName)) {
            return brokerAddressTable.get(brokerName);
        }

        List<BrokerData> brokerDataList = topicPublishInfo.getBrokerDataList();
        for (BrokerData brokerData : brokerDataList) {
            if (brokerData.getBrokerName().equals(brokerName)) {
                return brokerData.getAddressMap().get(Constant.MASTER_ID);
            }
        }

        throw new ClientException("can not find the address with broker [" + brokerName + "]");
    }

    public void setSendMessageTimeOut(long sendMessageTimeOut) {
        this.sendMessageTimeOut = sendMessageTimeOut;
    }

    public void setSendMessageRetryTimes(int sendMessageRetryTimes) {
        this.sendMessageRetryTimes = sendMessageRetryTimes < 1 ? 3 : sendMessageRetryTimes;
    }
}
