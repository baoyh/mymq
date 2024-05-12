package bao.study.mymq.broker.raft;

/**
 * @author baoyh
 * @since 2024/4/7 16:43
 */
public class Config {

    /**
     * 一个心跳包的周期，默认为 200ms
     */
    private int heartBeatTimeIntervalMs = 200;

    /**
     * 允许最大的 N 个心跳周期内未收到心跳包，状态为 Follower 的节点只有超过
     * maxHeartBeatLeak * heartBeatTimeIntervalMs 的时间内未收到主节点的心跳包，
     * 才会重新进入 Candidate 状态，重新下一轮的选举
     */
    private int maxHeartBeatLeak = 3;

    /**
     * 最大的发送投票的间隔，默认为 1000ms
     */
    private int maxVoteIntervalMs = 1000;

    /**
     * 最小的发送投票间隔时间，默认为 100ms
     */
    private int minVoteIntervalMs = 100;


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

    public int getMaxVoteIntervalMs() {
        return maxVoteIntervalMs;
    }

    public void setMaxVoteIntervalMs(int maxVoteIntervalMs) {
        this.maxVoteIntervalMs = maxVoteIntervalMs;
    }

    public int getMinVoteIntervalMs() {
        return minVoteIntervalMs;
    }

    public void setMinVoteIntervalMs(int minVoteIntervalMs) {
        this.minVoteIntervalMs = minVoteIntervalMs;
    }
}
