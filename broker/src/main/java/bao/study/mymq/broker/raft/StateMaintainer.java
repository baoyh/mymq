package bao.study.mymq.broker.raft;

import bao.study.mymq.common.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

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
    private volatile AtomicLong lastHeartBeatTime = new AtomicLong(-1L);

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
                logger.info("{} lastHeartBeatTime now is {}", memberState.getSelfId(), this.getLastHeartBeatTime());
                switch (memberState.getRole()) {
                    case FOLLOWER:
                        logger.info(memberState.getSelfId() + ": become follower");
                        maintainAsFollower();
                        break;
                    case CANDIDATE:
                        logger.info(memberState.getSelfId() + ": become candidate");
                        maintainAsCandidate();
                        break;
                    case LEADER:
                        logger.info(memberState.getSelfId() + ": become leader");
                        maintainAsLeader();
                        break;
                }
                Thread.sleep(30);
            } catch (Exception e) {
                logger.error("state maintainer error ", e);
            }
        }
    }

    /**
     * 1. 超过一定时间未收到心跳则变为 Candidate
     */
    private void maintainAsFollower() {
        if (System.currentTimeMillis() - lastHeartBeatTime.get() > (long) memberState.getConfig().getHeartBeatTimeIntervalMs() * memberState.getConfig().getMaxHeartBeatLeak()) {
            logger.info("{} will change to candidate, lastHeartBeatTime is {}, current time is {}", memberState.getSelfId(), lastHeartBeatTime.get(), System.currentTimeMillis());
            if (memberState.getRole() == Role.FOLLOWER) {
                changeRoleToCandidate(memberState.getTerm());
                logger.info("{} nextTimeToRequestVote is {}, {}", memberState.getSelfId(), nextTimeToRequestVote, new Date(nextTimeToRequestVote));
            }
        }
    }

    /**
     * 1. 发起投票
     * 2. 成功收到过半票数时变为 Leader
     * 3. 失败时再次发起新一轮投票
     */
    private void maintainAsCandidate() throws Exception {
        if (System.currentTimeMillis() < nextTimeToRequestVote) return;

        LeaderElector.VoteResult voteResult = leaderElector.callVote();
        if (voteResult == PASSED) {
            memberState.setRole(Role.LEADER);
            memberState.setLeaderId(memberState.getSelfId());
            return;
        }
        nextTimeToRequestVote = getNextTimeToRequestVote();
        logger.info("{} nextTimeToRequestVote is {}, {}", memberState.getSelfId(), nextTimeToRequestVote, new Date(nextTimeToRequestVote));
    }

    /**
     * 1. 定期向 follower 发送心跳
     */
    private void maintainAsLeader() throws Exception {
        if (System.currentTimeMillis() - lastHeartBeatTime.get() >= memberState.getConfig().getHeartBeatTimeIntervalMs()) {
            heartbeatProcessor.sendHeartBeats();
        }
    }

    public void changeRoleToFollower(long term) {
        memberState.setTerm(Math.max(term, memberState.getTerm()));
        memberState.setRole(Role.FOLLOWER);
        memberState.setLeaderId(null);
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
        this.lastHeartBeatTime.set(lastHeartBeatTime);
        logger.info("{} lastHeartBeatTime now is set to {}", memberState.getSelfId(), this.getLastHeartBeatTime());
    }

    public void setHeartbeatProcessor(HeartbeatProcessor heartbeatProcessor) {
        this.heartbeatProcessor = heartbeatProcessor;
    }

    public void setLeaderElector(LeaderElector leaderElector) {
        this.leaderElector = leaderElector;
    }

    public long getLastHeartBeatTime() {
        return lastHeartBeatTime.get();
    }
}
