package bao.study.mymq.common.transport;

import bao.study.mymq.common.transport.broker.BrokerData;
import bao.study.mymq.common.transport.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;

/**
 * @author baoyh
 * @since 2022/6/30 16:43
 */
public class TopicPublishInfo {

    private List<MessageQueue> messageQueueList = new ArrayList<>();

    private List<BrokerData> brokerDataList = new ArrayList<>();

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public List<BrokerData> getBrokerDataList() {
        return brokerDataList;
    }

    public void setBrokerDataList(List<BrokerData> brokerDataList) {
        this.brokerDataList = brokerDataList;
    }
}
