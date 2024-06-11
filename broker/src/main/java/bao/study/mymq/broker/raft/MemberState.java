package bao.study.mymq.broker.raft;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2024/4/7 14:01
 */
public class MemberState {

    private Config config;

    private volatile Role role = Role.FOLLOWER;

    private volatile String leaderId;

    private volatile String selfId;

    private volatile long term;

    private volatile String currVoteFor;

    private Map<String /*id*/, String /*address*/> nodes = new ConcurrentHashMap<>();

    private Map<String /*id*/, Boolean> liveNodes = new ConcurrentHashMap<>();

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public Map<String, String> getNodes() {
        return nodes;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getSelfId() {
        return selfId;
    }

    public void setSelfId(String selfId) {
        this.selfId = selfId;
    }

    public Map<String, Boolean> getLiveNodes() {
        return liveNodes;
    }

    public void setNodes(Map<String, String> nodes) {
        this.nodes = nodes;
    }

    public void setLiveNodes(Map<String, Boolean> liveNodes) {
        this.liveNodes = liveNodes;
    }

    public String getCurrVoteFor() {
        return currVoteFor;
    }

    public void setCurrVoteFor(String currVoteFor) {
        this.currVoteFor = currVoteFor;
    }

    @Override
    public String toString() {
        return "MemberState{" +
                "role=" + role +
                ", leaderId='" + leaderId + '\'' +
                ", selfId='" + selfId + '\'' +
                ", term=" + term +
                ", currVoteFor='" + currVoteFor + '\'' +
                '}';
    }
}
