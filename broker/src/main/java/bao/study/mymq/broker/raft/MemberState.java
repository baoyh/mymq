package bao.study.mymq.broker.raft;

import java.util.HashMap;
import java.util.Map;

/**
 * @author baoyh
 * @since 2024/4/7 14:01
 */
public class MemberState {

    private Config config;

    private volatile Role role = Role.FOLLOWER;

    private volatile String leaderId;

    private final Map<String /*id*/, String /*address*/> nodes = new HashMap<>();

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
}
