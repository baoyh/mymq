package bao.study.mymq.router.routeinfo;

import bao.study.mymq.common.Constant;
import bao.study.mymq.common.transport.TopicPublishInfo;
import bao.study.mymq.common.transport.body.RegisterBrokerBody;
import bao.study.mymq.common.transport.broker.BrokerData;
import bao.study.mymq.common.transport.message.MessageQueue;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.common.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author baoyh
 * @since 2022/6/17 17:14
 */
public class RouterInfoManager {

    private static final Logger log = LoggerFactory.getLogger(RouterInfoManager.class);

    private final Map<String /* topic */, TopicPublishInfo> topicTable = new HashMap<>();

    private final Map<String /* clusterName */, Map<String /* brokerName */, Integer /* index in brokerDataList */>> registered = new HashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public void registerBroker(RemotingCommand msg) {

        RegisterBrokerBody body = CommonCodec.decode(msg.getBody(), RegisterBrokerBody.class);

        registerTopic(body);
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

                boolean clusterRegistered = registered.containsKey(clusterName);
                if (hasTopic && clusterRegistered && registered.get(clusterName).containsKey(brokerName)) {
                    BrokerData brokerData = brokerDataList.get(registered.get(clusterName).get(brokerName));
                    brokerData.getAddressMap().put(brokerId, brokerAddress);
                    return;
                }

                BrokerData brokerData = new BrokerData(clusterName, brokerName);
                brokerData.getAddressMap().put(brokerId, brokerAddress);
                brokerDataList.add(brokerData);

                if (brokerId == Constant.MASTER_ID) {
                    List<MessageQueue> messageQueueList = topicPublishInfo.getMessageQueueList();
                    MessageQueue messageQueue = new MessageQueue(brokerName, topic);
                    messageQueueList.add(messageQueue);
                }

                Map<String, Integer> map = clusterRegistered ? registered.get(clusterName) : new HashMap<>();
                map.put(brokerName, brokerDataList.size() - 1);
                registered.put(clusterName, map);

                topicTable.put(topic, topicPublishInfo);

            }

            System.out.println();

        } catch (Exception e) {

            if (log.isErrorEnabled()) {
                log.error("register broker fail ", e);
            }

            lock.unlock();
        }

    }
}
