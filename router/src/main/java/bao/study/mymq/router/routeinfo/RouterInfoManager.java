package bao.study.mymq.router.routeinfo;

import bao.study.mymq.common.transport.MessageQueue;
import bao.study.mymq.common.transport.BrokerHeader;
import bao.study.mymq.common.transport.TopicPublishInfo;
import bao.study.mymq.common.utils.DataParseHelper;
import bao.study.mymq.remoting.common.RemotingCommand;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2022/6/17 17:14
 */
public class RouterInfoManager {

    private final Map<String /* broker name */, BrokerHeader> storeTable = new ConcurrentHashMap<>();

    private final Map<String /* cluster name */, Set<String /* broker name */>> clusterTable = new ConcurrentHashMap<>();

    private final Map<String /* topic */, TopicPublishInfo> topicTable = new ConcurrentHashMap<>();

    public void registerStore(RemotingCommand msg) {

        BrokerHeader brokerHeader = DataParseHelper.byte2Object(msg.getHeader(), BrokerHeader.class);
        storeTable.putIfAbsent(brokerHeader.getStoreName(), brokerHeader);
        Set<String> storeNameSet = clusterTable.getOrDefault(brokerHeader.getClusterName(), new HashSet<>());
        storeNameSet.add(brokerHeader.getStoreName());

        MessageQueue messageQueue = DataParseHelper.byte2Object(msg.getBody(), MessageQueue.class);


    }
}
