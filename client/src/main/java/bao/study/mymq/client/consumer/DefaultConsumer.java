package bao.study.mymq.client.consumer;

import bao.study.mymq.client.BrokerInstance;
import bao.study.mymq.client.Client;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.ServiceThread;
import bao.study.mymq.common.protocol.MessageExt;
import bao.study.mymq.common.protocol.TopicPublishInfo;
import bao.study.mymq.common.protocol.body.PullMessageBody;
import bao.study.mymq.common.protocol.body.SendMessageBackBody;
import bao.study.mymq.common.protocol.broker.BrokerData;
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

    private final Set<String> topics = new CopyOnWriteArraySet<>();

    private MessageListener messageListener;

    private final BrokerInstance brokerInstance = new BrokerInstance();

    private final ThreadLocal<Map<String /* brokerName */, Integer /* brokerIdIndex */>> consumerWhichBroker = ThreadLocal.withInitial(HashMap::new);

    private final Map<String /* brokerName */, Map<Long, String> /* address */> brokerTable = new ConcurrentHashMap<>();

    private final Map<String /* brokerName */, List<Long> /* brokerIds */> brokerIdTable = new ConcurrentHashMap<>();

    private final LinkedBlockingQueue<MessageQueue> pullRequestQueue = new LinkedBlockingQueue<>();

    private final long consumeTimeout = 3 * 1000L;

    @Override
    protected void doStart() {
        init();
        pull();
    }

    private void init() {

        for (String topic : topics) {

            TopicPublishInfo topicPublishInfo = brokerInstance.findTopicPublishInfo(topic, this);
            registerBrokerTable(topicPublishInfo.getBrokerDataList());

            List<MessageQueue> messageQueueList = topicPublishInfo.getMessageQueueList();
            pullRequestQueue.addAll(messageQueueList);
        }
    }

    private void pull() {
        PullMessageService pullMessageService = new PullMessageService();
        pullMessageService.start();
    }

    private void pullMessage(MessageQueue messageQueue) {

        PullMessageBody body = new PullMessageBody();
        body.setGroup(group);
        body.setTopic(messageQueue.getTopic());
        body.setQueueId(messageQueue.getQueueId());
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.PULL_MESSAGE, CommonCodec.encode(body));

        String address = selectOneBroker(messageQueue.getBrokerName());
        remotingClient.invokeAsync(address, remotingCommand, consumeTimeout, responseFuture -> {
            RemotingCommand responseCommand = responseFuture.getResponseCommand();
            switch (responseCommand.getCode()) {
                case ResponseCode.FOUND_MESSAGE:
                    byte[] response = responseCommand.getBody();
                    try {
                        if (response != null) {
                            List<MessageExt> messages = CommonCodec.decodeAsList(response, MessageExt.class);
                            ConsumeConcurrentlyStatus consumeConcurrentlyStatus = messageListener.consumerMessage(messages);
                            RemotingCommand command = sendMessageBack(consumeConcurrentlyStatus, messages);
                            remotingClient.invokeOneway(address, command, consumeTimeout);
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
    }

    private RemotingCommand sendMessageBack(ConsumeConcurrentlyStatus consumeConcurrentlyStatus, List<MessageExt> messages) {
        SendMessageBackBody sendMessageBackBody = new SendMessageBackBody();
        switch (consumeConcurrentlyStatus) {
            case RECONSUME_LATER:
                sendMessageBackBody.setStatus(false);
                break;
            case CONSUME_SUCCESS:
                MessageExt messageExt = messages.stream().max(Comparator.comparingLong(MessageExt::getOffset)).get();
                sendMessageBackBody.setStatus(true);
                sendMessageBackBody.setOffset(messageExt.getOffset());
                sendMessageBackBody.setTopic(messageExt.getTopic());
                sendMessageBackBody.setQueueId(messageExt.getQueueId());
                sendMessageBackBody.setGroup(messageExt.getGroup());
                break;
        }
        return RemotingCommandFactory.createRequestRemotingCommand(RequestCode.CONSUMER_SEND_MSG_BACK, CommonCodec.encode(sendMessageBackBody));
    }

    private String selectOneBroker(String brokerName) {
        Map<String, Integer> whichBroker = consumerWhichBroker.get();
        Integer index = whichBroker.get(brokerName);
        if (index == null) {
            index = 0;
        }

        List<Long> slaves = brokerIdTable.get(brokerName);
        Long brokerId = slaves.get(index);
        String address = brokerTable.get(brokerName).get(brokerId);

        index++;
        if (index == slaves.size()) {
            index = 0;
        }
        whichBroker.put(brokerName, index);
        consumerWhichBroker.set(whichBroker);

        return address;
    }

    private void registerBrokerTable(List<BrokerData> brokerDataList) {
        for (BrokerData brokerData : brokerDataList) {
            List<Long> brokerIds = new ArrayList<>();
            for (Long brokerId : brokerData.getAddressMap().keySet()) {
                if (brokerId != 0L) {
                    // 当存在 slave 时, 只有 slave 可以进行消费
                    brokerIds.add(brokerId);
                }
            }
            if (brokerIds.isEmpty()) {
                // 当不存在 slave, master 需承担消费
                brokerIds.add(Constant.MASTER_ID);
            }
            brokerIdTable.put(brokerData.getBrokerName(), brokerIds);
            brokerTable.put(brokerData.getBrokerName(), brokerData.getAddressMap());
        }
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
}
