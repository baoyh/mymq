package bao.study.mymq.router.routeinfo;

import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.TopicPublishInfo;
import bao.study.mymq.common.protocol.body.RegisterBrokerBody;
import bao.study.mymq.common.protocol.broker.BrokerData;
import bao.study.mymq.common.protocol.message.MessageQueue;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author baoyh
 * @since 2022/6/17 17:14
 */
public class RouterInfoManager {

    private static final Logger log = LoggerFactory.getLogger(RouterInfoManager.class);

    private final Map<String /* topic */, TopicPublishInfo> topicTable = new HashMap<>();

    private final Map<BrokerDataIndex, Integer /* index in brokerDataList */> registered = new HashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public RemotingCommand registerBroker(RemotingCommand msg) {

        RegisterBrokerBody body = CommonCodec.decode(msg.getBody(), RegisterBrokerBody.class);

        registerTopic(body);

        return RemotingCommandFactory.createResponseRemotingCommand(ResponseCode.SUCCESS, null);
    }

    private void registerTopic(RegisterBrokerBody body) {
        Set<String> topics = body.getTopics();

        try {
            lock.lock();

            for (String topic : topics) {

                boolean hasTopic = topicTable.containsKey(topic);

                TopicPublishInfo topicPublishInfo = hasTopic ? topicTable.get(topic) : new TopicPublishInfo();
                List<BrokerData> brokerDataList = topicPublishInfo.getBrokerDataList();

                String clusterName = body.getClusterName();
                String brokerName = body.getBrokerName();
                String brokerAddress = body.getBrokerAddress();
                long brokerId = body.getBrokerId();
                BrokerDataIndex index = new BrokerDataIndex(clusterName, brokerName, topic);

                if (hasTopic && registered.containsKey(index)) {
                    BrokerData brokerData = brokerDataList.get(registered.get(index));
                    brokerData.getAddressMap().put(brokerId, brokerAddress);
                    continue;
                }

                BrokerData brokerData = new BrokerData(clusterName, brokerName);
                brokerData.getAddressMap().put(brokerId, brokerAddress);
                brokerDataList.add(brokerData);

                if (brokerId == Constant.MASTER_ID) {
                    List<MessageQueue> messageQueueList = topicPublishInfo.getMessageQueueList();
                    MessageQueue messageQueue = new MessageQueue(brokerName, topic);
                    messageQueueList.add(messageQueue);
                }

                registered.put(index, brokerDataList.size() - 1);

                topicTable.put(topic, topicPublishInfo);

            }

        } catch (Exception e) {

            if (log.isErrorEnabled()) {
                log.error("register broker fail ", e);
            }

        } finally {
            lock.unlock();
        }

    }

    public RemotingCommand getRouteByTopic(RemotingCommand msg) {
        String topic = CommonCodec.decode(msg.getBody(), String.class);
        TopicPublishInfo topicPublishInfo = topicTable.get(topic);
        return RemotingCommandFactory.createResponseRemotingCommand(ResponseCode.SUCCESS, CommonCodec.encode(topicPublishInfo));
    }

    private static class BrokerDataIndex {

        private final String clusterName;

        private final String brokerName;

        private final String topic;

        public BrokerDataIndex(String clusterName, String brokerName, String topic) {
            this.clusterName = clusterName;
            this.brokerName = brokerName;
            this.topic = topic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BrokerDataIndex that = (BrokerDataIndex) o;
            return clusterName.equals(that.clusterName) && brokerName.equals(that.brokerName) && topic.equals(that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterName, brokerName, topic);
        }
    }
}
