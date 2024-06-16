package bao.study.mymq.client.consumer;

import bao.study.mymq.client.BrokerInstance;
import bao.study.mymq.client.Client;
import bao.study.mymq.client.ClientException;
import bao.study.mymq.client.consistenthash.AllocateMessageQueueConsistentHash;
import bao.study.mymq.common.ServiceThread;
import bao.study.mymq.common.protocol.MessageExt;
import bao.study.mymq.common.protocol.TopicPublishInfo;
import bao.study.mymq.common.protocol.body.PullMessageBody;
import bao.study.mymq.common.protocol.body.SendMessageBackBody;
import bao.study.mymq.common.protocol.broker.BrokerData;
import bao.study.mymq.common.protocol.client.ConsumerData;
import bao.study.mymq.common.protocol.message.MessageQueue;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author baoyh
 * @since 2022/10/27 10:39
 */
public class DefaultConsumer extends Client implements Consumer {

    private static final Logger log = LoggerFactory.getLogger(DefaultConsumer.class);

    private String group;

    private final String consumerId = UUID.randomUUID().toString();

    private final Set<String> topics = new CopyOnWriteArraySet<>();

    private MessageListener messageListener;

    private final BrokerInstance brokerInstance = new BrokerInstance();

    private final Map<MessageQueue, Boolean /* valid */> messageQueueTable = new ConcurrentHashMap<>();

    private final AllocateMessageQueueConsistentHash messageQueueBalance = new AllocateMessageQueueConsistentHash(100);

    private final Map<String /* brokerName */, AllocateMessageQueueConsistentHash> consistentHashTable = new ConcurrentHashMap<>();

    private final Map<MessageQueue, String /* brokerAddress */> consumeWhichBroker = new ConcurrentHashMap<>();

    private final Map<String /* brokerName */, ConcurrentHashMap<Long, String> /* address */> brokerTable = new ConcurrentHashMap<>();

    private final LinkedBlockingQueue<MessageQueue> pullRequestQueue = new LinkedBlockingQueue<>();

    private final long rpcTimeoutMills = 3 * 1000L;

    @Override
    protected void doStart() {
        check();
        register();
        init();
        pull();
        handleBroker();
        loadBalance();
    }

    private void check() {
        if (group == null) {
            throw new ClientException("group is null");
        }
        if (topics.isEmpty()) {
            throw new ClientException("topics is empty");
        }
        if (messageListener == null) {
            throw new ClientException("messageListener is null");
        }
    }

    private void register() {
        RemotingCommand request = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.REGISTER_CONSUMER, CommonCodec.encode(new ConsumerData(consumerId, group, topics)));
        remotingClient.invokeOneway(getRouterAddress(), request, rpcTimeoutMills);
    }

    private void init() {

        for (String topic : topics) {

            TopicPublishInfo topicPublishInfo = brokerInstance.findTopicPublishInfo(topic, this);
            registerBrokerTable(topicPublishInfo.getBrokerDataList());
            initConsistentHashTable();

            List<MessageQueue> messageQueueList = topicPublishInfo.getMessageQueueList();
            loadBalanceImpl(messageQueueList, topic, group);
        }
    }

    private void pull() {
        PullMessageService pullMessageService = new PullMessageService();
        pullMessageService.start();
    }

    private void handleBroker() {
        BrokerHandler brokerHandler = new BrokerHandler();
        brokerHandler.start();
    }

    private void loadBalance() {
        ConsumerLoadBalance consumerLoadBalance = new ConsumerLoadBalance();
        consumerLoadBalance.start();
    }

    private void pullMessage(MessageQueue messageQueue) {

        if (!messageQueueTable.containsKey(messageQueue)) {
            return;
        }
        if (!messageQueueTable.get(messageQueue)) {
            pullRequestQueue.remove(messageQueue);
            return;
        }

        PullMessageBody body = new PullMessageBody();
        body.setGroup(group);
        body.setTopic(messageQueue.getTopic());
        body.setQueueId(messageQueue.getQueueId());
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.PULL_MESSAGE, CommonCodec.encode(body));

        String address = selectOneBroker(messageQueue);
        try {
            log.info("pull message from broker: {}, message queue: {}", address, messageQueue.getQueueId());
            remotingClient.invokeAsync(address, remotingCommand, rpcTimeoutMills, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();
                if (responseCommand == null) {
                    removeNode(messageQueue, address);
                    pullRequestQueue.add(messageQueue);
                    return;
                }
                switch (responseCommand.getCode()) {
                    case ResponseCode.FOUND_MESSAGE:
                        byte[] response = responseCommand.getBody();
                        try {
                            if (response != null) {
                                List<MessageExt> messages = CommonCodec.decodeAsList(response, MessageExt.class);
                                ConsumeConcurrentlyStatus consumeConcurrentlyStatus = messageListener.consumerMessage(messages);
                                sendMessageBackToAllBroker(consumeConcurrentlyStatus, messages);
                            }
                        } finally {
                            pullRequestQueue.add(messageQueue);
                        }

                        break;
                    case ResponseCode.NOT_FOUND_MESSAGE:
                        pullRequestQueue.add(messageQueue);
                        break;
                    default:
                        break;
                }
            });
        } catch (Throwable ex) {
            log.error(ex.getMessage(), ex);
            removeNode(messageQueue, address);
            pullRequestQueue.add(messageQueue);
        }
    }

    private void sendMessageBackToAllBroker(ConsumeConcurrentlyStatus consumeConcurrentlyStatus, List<MessageExt> messages) {
        if (consumeConcurrentlyStatus != ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
            return;
        }
        SendMessageBackBody sendMessageBackBody = new SendMessageBackBody();
        MessageExt messageExt = messages.stream().max(Comparator.comparingLong(MessageExt::getOffset)).get();
        sendMessageBackBody.setStatus(true);
        sendMessageBackBody.setOffset(messageExt.getOffset());
        sendMessageBackBody.setTopic(messageExt.getTopic());
        sendMessageBackBody.setQueueId(messageExt.getQueueId());
        sendMessageBackBody.setGroup(messageExt.getGroup());
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.CONSUMER_SEND_MSG_BACK, CommonCodec.encode(sendMessageBackBody));

        brokerTable.get(messageExt.getBrokerName()).forEach((brokerId, address) -> {
            try {
                remotingClient.invokeOneway(address, remotingCommand, rpcTimeoutMills);
            } catch (Throwable ex) {
                log.error(ex.getMessage(), ex);
            }
        });
    }


    private String selectOneBroker(MessageQueue messageQueue) {
        String whichBroker = consumeWhichBroker.get(messageQueue);
        if (whichBroker != null) {
            return whichBroker;
        }
        String address = consistentHashTable.get(messageQueue.getBrokerName()).get(messageQueue.getKey());
        consumeWhichBroker.put(messageQueue, address);
        return address;
    }

    private void loadBalanceImpl(List<MessageQueue> allMessageQueue, String topic, String group) {
        List<ConsumerData> consumers = findConsumersByGroup(group);
        Set<String> consumerIds = new HashSet<>();
        consumerIds.add(this.consumerId);
        consumers.forEach(consumer -> {
            if (consumer.getTopics().contains(topic)) {
                consumerIds.add(consumer.getConsumerId());
            }
        });

        for (String consumerId : consumerIds) {
            if (!messageQueueBalance.contains(consumerId)) {
                messageQueueBalance.add(consumerId);
            }
        }

        for (MessageQueue messageQueue : allMessageQueue) {
            String consumerId = messageQueueBalance.get(messageQueue.toString());
            if (this.consumerId.equals(consumerId)) {
                Boolean valid = messageQueueTable.get(messageQueue);
                if (valid == null || !valid) {
                    messageQueueTable.put(messageQueue, true);
                    pullRequestQueue.add(messageQueue);
                }
            } else {
                messageQueueTable.remove(messageQueue);
            }
        }
    }

    private List<ConsumerData> findConsumersByGroup(String group) {
        RemotingCommand request = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.QUERY_CONSUMERS_BY_GROUP, CommonCodec.encode(group));
        RemotingCommand response = remotingClient.invokeSync(getRouterAddress(), request, rpcTimeoutMills);
        return CommonCodec.decodeAsList(response.getBody(), ConsumerData.class);
    }

    private void registerBrokerTable(List<BrokerData> brokerDataList) {
        for (BrokerData brokerData : brokerDataList) {
            brokerTable.put(brokerData.getBrokerName(), new ConcurrentHashMap<>(brokerData.getAddressMap()));
        }
    }

    private void initConsistentHashTable() {
        Set<String> brokerNames = brokerTable.keySet();
        for (String brokerName : brokerNames) {
            AllocateMessageQueueConsistentHash consistentHash = new AllocateMessageQueueConsistentHash(100);
            brokerTable.get(brokerName).forEach((key, value) -> consistentHash.add(value));
            consistentHashTable.put(brokerName, consistentHash);
        }
    }

    private void removeNode(MessageQueue messageQueue, String address) {
        consumeWhichBroker.remove(messageQueue);
        consistentHashTable.get(messageQueue.getBrokerName()).remove(address);
        brokerTable.get(messageQueue.getBrokerName()).forEach((k, v) -> {
            if (v.equals(address)) {
                brokerTable.get(messageQueue.getBrokerName()).remove(k);
            }
        });
    }

    public BrokerData queryAliveBrokers(String brokerName) {
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.QUERY_ALIVE_BROKERS, CommonCodec.encode(brokerName));
        RemotingCommand response = remotingClient.invokeSync(getRouterAddress(), remotingCommand, rpcTimeoutMills);
        return CommonCodec.decode(response.getBody(), BrokerData.class);
    }

    @Override
    protected void doShutdown() {
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    public void subscribe(String topic) {
        assert topic != null;
        topics.add(topic);
    }

    @Override
    public void registerMessageListener(MessageListener messageListener) {
        assert messageListener != null;
        this.messageListener = messageListener;
    }


    class PullMessageService extends ServiceThread {

        @Override
        public String getServiceName() {
            return PullMessageService.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    MessageQueue messageQueue = pullRequestQueue.take();
                    pullMessage(messageQueue);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    class BrokerHandler extends ServiceThread {

        @Override
        public String getServiceName() {
            return BrokerHandler.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    waitForRunning(1000);
                    handleBrokerTable();
                } catch (Throwable ex) {
                    log.error(ex.getMessage(), ex);
                }
            }
        }

        private void handleBrokerTable() {
            Map<String, BrokerData> map = new HashMap<>();
            for (Map.Entry<String, ConcurrentHashMap<Long, String>> entry : brokerTable.entrySet()) {
                String brokerName = entry.getKey();
                BrokerData brokers = queryAliveBrokers(brokerName);
                map.put(brokerName, brokers);
            }

            brokerTable.forEach((k, v) -> {
                for (Long brokerId : v.keySet()) {
                    if (!map.get(k).getAddressMap().containsKey(brokerId)) {
                        brokerTable.get(k).remove(brokerId);
                    }
                }
            });

            map.forEach((k, v) -> {
                v.getAddressMap().forEach((brokerId, address) -> brokerTable.get(k).put(brokerId, address));
            });
        }

    }

    class ConsumerLoadBalance extends ServiceThread {

        @Override
        public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    waitForRunning(1000);
                    doLoadBalance();
                } catch (Throwable ex) {
                    log.error(ex.getMessage(), ex);
                }
            }
        }

        public void doLoadBalance() {
            for (String topic : topics) {
                TopicPublishInfo topicPublishInfo = brokerInstance.getTopicPublishInfoTable().get(topic);
                List<MessageQueue> messageQueueList = topicPublishInfo.getMessageQueueList();
                loadBalanceImpl(messageQueueList, topic, group);
            }
        }
    }
}
