package bao.study.mymq.broker.raft;

/**
 * @author baoyh
 * @since 2024/4/7 16:43
 */
public class Config {

    /**
     * 一个心跳包的周期，默认为 2s
     */
    private int heartBeatTimeIntervalMs = 2000;

    /**
     * 允许最大的 N 个心跳周期内未收到心跳包，状态为 Follower 的节点只有超过
     * maxHeartBeatLeak * heartBeatTimeIntervalMs 的时间内未收到主节点的心跳包，
     * 才会重新进入 Candidate 状态，重新下一轮的选举
     */
    private int maxHeartBeatLeak = 3;

    private long rpcTimeoutMillis = 3000L;

    public int getHeartBeatTimeIntervalMs() {
        return heartBeatTimeIntervalMs;
    }

    public void setHeartBeatTimeIntervalMs(int heartBeatTimeIntervalMs) {
        this.heartBeatTimeIntervalMs = heartBeatTimeIntervalMs;
    }

    public int getMaxHeartBeatLeak() {
        return maxHeartBeatLeak;
    }

    public void setMaxHeartBeatLeak(int maxHeartBeatLeak) {
        this.maxHeartBeatLeak = maxHeartBeatLeak;
    }

    public long getRpcTimeoutMillis() {
        return rpcTimeoutMillis;
    }

    public void setRpcTimeoutMillis(long rpcTimeoutMillis) {
        this.rpcTimeoutMillis = rpcTimeoutMillis;
    }
}
