package bao.study.mymq.router.routeinfo;

import bao.study.mymq.common.Constant;
import bao.study.mymq.common.ServiceThread;
import bao.study.mymq.common.protocol.TopicPublishInfo;
import bao.study.mymq.common.protocol.body.RegisterBrokerBody;
import bao.study.mymq.common.protocol.broker.BrokerData;
import bao.study.mymq.common.protocol.broker.Heartbeat;
import bao.study.mymq.common.protocol.message.MessageQueue;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author baoyh
 * @since 2022/6/17 17:14
 */
public class RouterInfoManager {

    private static final Logger log = LoggerFactory.getLogger(RouterInfoManager.class);

    private final Map<String /* topic */, TopicPublishInfo> topicTable = new ConcurrentHashMap<>();

    private final Map<BrokerDataIndex, Integer /* index in brokerDataList */> registered = new ConcurrentHashMap<>();

    private final Map<String /* brokerName */, BrokerData> brokerTable = new ConcurrentHashMap<>();

    private final Map<String /* brokerName */, ConcurrentHashMap<Long /* brokerId */, Long /* lastHeartbeatTime */>> aliveBrokerTable = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    private final AliveBrokerManager aliveBrokerManager = new AliveBrokerManager();

    public void start() {
        aliveBrokerManager.start();
    }

    public void shutdown() {
        aliveBrokerManager.shutdown();
    }

    public RemotingCommand registerBroker(RemotingCommand msg) {

        RegisterBrokerBody body = CommonCodec.decode(msg.getBody(), RegisterBrokerBody.class);

        registerTopic(body);

        return RemotingCommandFactory.createResponseRemotingCommand(ResponseCode.SUCCESS, null);
    }

    private void registerTopic(RegisterBrokerBody body) {
        Set<String> topics = body.getTopics().keySet();

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
                    for (int i = 0; i < body.getTopics().get(topic); i++) {
                        MessageQueue messageQueue = new MessageQueue(brokerName, topic, i);
                        messageQueueList.add(messageQueue);
                    }
                }

                registered.put(index, brokerDataList.size() - 1);
                topicTable.put(topic, topicPublishInfo);
                brokerTable.put(brokerName, brokerData);
                updateAliveBrokerTable(brokerName, brokerId);
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

    public RemotingCommand handleHeartbeat(RemotingCommand msg) {
        Heartbeat heartbeat = CommonCodec.decode(msg.getBody(), Heartbeat.class);
        BrokerData brokerData = brokerTable.get(heartbeat.getBrokerName());
        updateAliveBrokerTable(heartbeat.getBrokerName(), heartbeat.getBrokerId());
        return RemotingCommandFactory.createResponseRemotingCommand(ResponseCode.SUCCESS, CommonCodec.encode(brokerData));
    }

    private void updateAliveBrokerTable(String brokerName, long brokerId) {
        ConcurrentHashMap<Long, Long> brokers = aliveBrokerTable.getOrDefault(brokerName, new ConcurrentHashMap<>());
        brokers.put(brokerId, System.currentTimeMillis());
        aliveBrokerTable.put(brokerName, brokers);
    }

    public RemotingCommand queryAliveBrokers(RemotingCommand msg) {
        String brokerName = CommonCodec.decode(msg.getBody(), String.class);
        ConcurrentHashMap<Long, Long> brokers = aliveBrokerTable.get(brokerName);
        if (brokers == null || brokers.isEmpty()) {
            return RemotingCommandFactory.createResponseRemotingCommand(ResponseCode.NOT_FOUND_BROKER, null);
        }

        BrokerData brokerData = new BrokerData(null, brokerName);
        brokers.forEach((k, v) -> brokerData.getAddressMap().put(k, brokerTable.get(brokerName).getAddressMap().get(k)));
        return RemotingCommandFactory.createResponseRemotingCommand(ResponseCode.SUCCESS, CommonCodec.encode(brokerData));
    }

    private class AliveBrokerManager extends ServiceThread {

        @Override
        public String getServiceName() {
            return AliveBrokerManager.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    checkHeartbeat();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                } finally {
                    waitForRunning(100);
                }
            }
        }

        public void checkHeartbeat() {
            Set<Map.Entry<String, ConcurrentHashMap<Long, Long>>> entries = aliveBrokerTable.entrySet();
            for (Map.Entry<String, ConcurrentHashMap<Long, Long>> entry : entries) {
                entry.getValue().entrySet().removeIf(e -> e.getValue() < System.currentTimeMillis() + 100 - 3 * 200);
            }
        }
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
