package bao.study.mymq.broker.raft;

/**
 * 状态机
 *
 * @author baoyh
 * @since 2024/4/7 13:59
 */
public class StateMaintainer {

    private MemberState memberState;

    private Config config;

    /**
     * 上一次收到心跳的时间戳
     */
    private volatile long lastHeartBeatTime = -1L;

    public void startup() {
        switch (memberState.getRole()) {
            case FOLLOWER:
                maintainAsFollower();
            case CANDIDATE:
                maintainAsCandidate();
            case LEADER:
                maintainAsLeader();
        }
    }

    /**
     * 1. 超过一定时间未收到心跳则变为 Candidate
     */
    private void maintainAsFollower() {
        if (System.currentTimeMillis() - lastHeartBeatTime > (long) config.getHeartBeatTimeIntervalMs() * config.getMaxHeartBeatLeak()) {
            memberState.setRole(Role.CANDIDATE);
        }
    }

    /**
     * 1. 发起投票
     * 2. 成功收到过半票数时变为 Leader
     * 3. 失败时再次发起新一轮投票
     */
    private void maintainAsCandidate() {

    }

    /**
     * 1. 定期向 follower 发送心跳
     */
    private void maintainAsLeader() {
        synchronized (memberState) {
            sendHeartBeats();
        }
    }

    private void sendHeartBeats() {
        memberState.getNodes().forEach((k, v) -> {


                }
        );
    }
}
