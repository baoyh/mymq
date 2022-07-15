package bao.study.mymq.common.transport;

import java.util.ArrayList;
import java.util.List;

/**
 * @author baoyh
 * @since 2022/6/30 16:43
 */
public class TopicPublishInfo {

    private List<BrokerData> brokerDataList = new ArrayList<>();

    private List<QueueData> queueDataList = new ArrayList<>();

    public List<BrokerData> getStoreDataList() {
        return brokerDataList;
    }

    public void setStoreDataList(List<BrokerData> brokerDataList) {
        this.brokerDataList = brokerDataList;
    }

    public List<QueueData> getQueueDataList() {
        return queueDataList;
    }

    public void setQueueDataList(List<QueueData> queueDataList) {
        this.queueDataList = queueDataList;
    }
}
