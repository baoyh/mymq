package bao.study.mymq.client.consumer;

import bao.study.mymq.client.BrokerInstance;
import bao.study.mymq.client.Client;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.MessageExt;
import bao.study.mymq.common.protocol.TopicPublishInfo;
import bao.study.mymq.common.protocol.body.PullMessageBody;
import bao.study.mymq.common.protocol.broker.BrokerData;
import bao.study.mymq.common.protocol.message.MessageQueue;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author baoyh
 * @since 2022/10/27 10:39
 */
public class DefaultConsumer extends Client implements Consumer {

    private String group;

    private final Set<String> topics = new CopyOnWriteArraySet<>();

    private MessageListener messageListener;

    private final BrokerInstance brokerInstance = new BrokerInstance();

    private final ThreadLocal<Map<String /* brokerName */, Integer /* brokerIdIndex */>> consumerWhichBroker = ThreadLocal.withInitial(HashMap::new);

    private final Map<String /* brokerName */, Map<Long, String> /* address */> brokerTable = new ConcurrentHashMap<>();

    private final Map<String /* brokerName */, List<Long> /* brokerIds */> brokerIdTable = new ConcurrentHashMap<>();

    private long consumeTimeout = 3 * 1000L;

    @Override
    protected void doStart() {
        for (String topic : topics) {
            TopicPublishInfo topicPublishInfo = brokerInstance.findTopicPublishInfo(topic, this);

            registerBrokerTable(topicPublishInfo.getBrokerDataList());

            List<MessageQueue> messageQueueList = topicPublishInfo.getMessageQueueList();

            for (MessageQueue messageQueue : messageQueueList) {
                pullMessage(messageQueue);
            }
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

    private void pullMessage(MessageQueue messageQueue) {

        PullMessageBody body = new PullMessageBody();
        body.setGroup(group);
        body.setTopic(messageQueue.getTopic());
        body.setQueueId(messageQueue.getQueueId());
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.PULL_MESSAGE, CommonCodec.encode(body));

        String address = selectOneBroker(messageQueue.getBrokerName());
        remotingClient.invokeAsync(address, remotingCommand, consumeTimeout, responseFuture -> {
            byte[] response = responseFuture.getResponseCommand().getBody();
            if (response != null) {
                List<MessageExt> messages = CommonCodec.decodeAsList(response, MessageExt.class);
                messageListener.consumerMessage(messages);
            }
        });
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

}
