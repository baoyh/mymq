package bao.study.mymq.client;

import bao.study.mymq.common.protocol.TopicPublishInfo;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2023/2/4 10:31
 */
public class BrokerInstance {

    private final Map<String /* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();

    public TopicPublishInfo findTopicPublishInfo(String topic, Client client) {

        if (topicPublishInfoTable.containsKey(topic)) {
            return topicPublishInfoTable.get(topic);
        }

        RemotingCommand request = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.GET_ROUTE_BY_TOPIC, CommonCodec.encode(topic));
        RemotingCommand response = client.remotingClient.invokeSync(client.getRouterAddress(), request, 3000);

        TopicPublishInfo topicPublishInfo = CommonCodec.decode(response.getBody(), TopicPublishInfo.class);
        topicPublishInfoTable.put(topic, topicPublishInfo);
        return topicPublishInfo;
    }

    public Map<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }
}
