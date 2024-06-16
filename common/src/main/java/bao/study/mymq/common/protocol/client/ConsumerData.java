package bao.study.mymq.common.protocol.client;

import java.util.Set;

/**
 * @author baoyh
 * @since 2024/6/16 15:04
 */
public class ConsumerData {

    private String consumerId;

    private String group;

    private Set<String> topics;

    public ConsumerData(String consumerId, String group, Set<String> topics) {
        this.consumerId = consumerId;
        this.group = group;
        this.topics = topics;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }
}
