package bao.study.mymq.broker.raft;

import bao.study.mymq.common.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static bao.study.mymq.broker.raft.LeaderElector.VoteResult.*;

/**
 * 状态机
 *
 * @author baoyh
 * @since 2024/4/7 13:59
 */
public class StateMaintainer extends ServiceThread {

    private static final Logger logger = LoggerFactory.getLogger(StateMaintainer.class);

    private HeartbeatProcessor heartbeatProcessor;

    private LeaderElector leaderElector;

    private final MemberState memberState;

    /**
     * 上一次收到心跳的时间戳
     */
    private volatile long lastHeartBeatTime = -1L;

    private volatile long nextTimeToRequestVote = -1L;

    public StateMaintainer(MemberState memberState) {
        this.memberState = memberState;
    }

    @Override
    public String getServiceName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                switch (memberState.getRole()) {
                    case FOLLOWER:
                        logger.debug(memberState.getSelfId() + ": become follower");
                        maintainAsFollower();
                        break;
                    case CANDIDATE:
                        logger.debug(memberState.getSelfId() + ": become candidate");
                        maintainAsCandidate();
                        break;
                    case LEADER:
                        logger.debug(memberState.getSelfId() + ": become leader");
                        maintainAsLeader();
                        break;
                }
                Thread.sleep(50);
            } catch (Exception e) {
                logger.error("state maintainer error ", e);
            }
        }
    }

    /**
     * 1. 超过一定时间未收到心跳则变为 Candidate
     */
    private void maintainAsFollower() {
        if (System.currentTimeMillis() - lastHeartBeatTime > (long) memberState.getConfig().getHeartBeatTimeIntervalMs() * memberState.getConfig().getMaxHeartBeatLeak()) {
            changeRoleToCandidate(memberState.getTerm());
        }
    }

    /**
     * 1. 发起投票
     * 2. 成功收到过半票数时变为 Leader
     * 3. 失败时再次发起新一轮投票
     */
    private void maintainAsCandidate() throws Exception {
        logger.info("nextTimeToRequestVote is {}, current time is {}", nextTimeToRequestVote, System.currentTimeMillis());
        if (System.currentTimeMillis() < nextTimeToRequestVote) return;

        LeaderElector.VoteResult voteResult = leaderElector.callVote();
        if (voteResult == PASSED) {
            memberState.setRole(Role.LEADER);
            memberState.setLeaderId(memberState.getSelfId());
            return;
        }
        nextTimeToRequestVote = getNextTimeToRequestVote();
    }

    /**
     * 1. 定期向 follower 发送心跳
     */
    private void maintainAsLeader() throws Exception {
        if (System.currentTimeMillis() - lastHeartBeatTime >= memberState.getConfig().getHeartBeatTimeIntervalMs()) {
            heartbeatProcessor.sendHeartBeats();
        }
    }

    public void changeRoleToFollower(long term, String leaderId) {
        memberState.setTerm(Math.max(term, memberState.getTerm()));
        memberState.setRole(Role.FOLLOWER);
        memberState.setLeaderId(leaderId);
        memberState.setCurrVoteFor(null);
    }

    public void changeRoleToCandidate(long term) {
        changeRoleToCandidate(term, getNextTimeToRequestVote());
    }

    public void changeRoleToCandidate(long term, long nextTimeToRequestVote) {
        memberState.setTerm(Math.max(term, memberState.getTerm()));
        memberState.setLeaderId(null);
        memberState.setRole(Role.CANDIDATE);
        memberState.setCurrVoteFor(null);
        this.nextTimeToRequestVote = nextTimeToRequestVote;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.memberState.setRole(Role.FOLLOWER);
    }

    private long getNextTimeToRequestVote() {
        return System.currentTimeMillis() + memberState.getConfig().getMinVoteIntervalMs() + new Random().nextInt(memberState.getConfig().getMaxVoteIntervalMs() - memberState.getConfig().getMinVoteIntervalMs());
    }

    public MemberState getMemberState() {
        return memberState;
    }

    public void setLastHeartBeatTime(long lastHeartBeatTime) {
        this.lastHeartBeatTime = lastHeartBeatTime;
    }

    public void setHeartbeatProcessor(HeartbeatProcessor heartbeatProcessor) {
        this.heartbeatProcessor = heartbeatProcessor;
    }

    public void setLeaderElector(LeaderElector leaderElector) {
        this.leaderElector = leaderElector;
    }

    public long getLastHeartBeatTime() {
        return lastHeartBeatTime;
    }

    @Override
    public String toString() {
        return "StateMaintainer{" +
                "heartbeatProcessor=" + heartbeatProcessor +
                ", leaderElector=" + leaderElector +
                ", memberState=" + memberState +
                ", lastHeartBeatTime=" + lastHeartBeatTime +
                ", nextTimeToRequestVote=" + nextTimeToRequestVote +
                '}';
    }
}
