package bao.study.mymq.broker.raft;

import bao.study.mymq.broker.raft.protocol.ClientProtocol;
import bao.study.mymq.broker.raft.protocol.ServerProtocol;
import bao.study.mymq.common.ServiceThread;
import bao.study.mymq.common.protocol.raft.HeartBeat;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.code.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 状态机
 *
 * @author baoyh
 * @since 2024/4/7 13:59
 */
public class StateMaintainer extends ServiceThread {

    private static final Logger logger = LoggerFactory.getLogger(StateMaintainer.class);

    private LeaderElector leaderElector;

    private ClientProtocol clientProtocol;

    private ServerProtocol serverProtocol;

    private MemberState memberState;

    private Config config;

    /**
     * 上一次收到心跳的时间戳
     */
    private volatile long lastHeartBeatTime = -1L;


    @Override
    public String getServiceName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void run() {
        while (true) {
            try {
                logger.info("start state maintainer");
                switch (memberState.getRole()) {
                    case FOLLOWER:
                        maintainAsFollower();
                        break;
                    case CANDIDATE:
                        maintainAsCandidate();
                        break;
                    case LEADER:
                        maintainAsLeader();
                        break;
                }
                Thread.sleep(100);
            } catch (Exception e) {
                logger.error("state maintainer error ", e);
            }

        }
    }

    /**
     * 1. 超过一定时间未收到心跳则变为 Candidate
     */
    private void maintainAsFollower() {
        if (System.currentTimeMillis() - lastHeartBeatTime > (long) config.getHeartBeatTimeIntervalMs() * config.getMaxHeartBeatLeak()) {
            memberState.setRole(Role.CANDIDATE);
            memberState.setLeaderId(null);
        }
    }

    /**
     * 1. 发起投票
     * 2. 成功收到过半票数时变为 Leader
     * 3. 失败时再次发起新一轮投票
     */
    private void maintainAsCandidate() {
        leaderElector.callVote();
    }

    /**
     * 1. 定期向 follower 发送心跳
     */
    private void maintainAsLeader() throws Exception {
        sendHeartBeats();
    }

    /**
     * 只有 leader 会主动发起 heartbeats
     */
    private void sendHeartBeats() throws InterruptedException {
        AtomicInteger all = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();
        AtomicLong maxTerm = new AtomicLong(memberState.getTerm());
        AtomicBoolean inconsistentLeader = new AtomicBoolean(false);

        for (Map.Entry<String, String> entry : memberState.getNodes().entrySet()) {
            if (entry.getKey().equals(memberState.getLeaderId())) {
                continue;
            }

            HeartBeat heartBeat = new HeartBeat();
            heartBeat.setCode(RequestCode.SEND_HEARTBEAT);
            heartBeat.setLeaderId(memberState.getLeaderId());
            heartBeat.setRemoteId(entry.getKey());
            heartBeat.setLocalId(memberState.getSelfId());
            heartBeat.setTerm(memberState.getTerm());

            CountDownLatch countDownLatch = new CountDownLatch(1);
            CompletableFuture<HeartBeat> future = clientProtocol.sendHeartBeat(heartBeat);
            future.whenComplete((HeartBeat response, Throwable ex) -> {
                try {
                    all.incrementAndGet();
                    if (ex != null) {
                        // 抛出异常视为该节点不可用
                        memberState.getLiveNodes().remove(entry.getKey());
                        throw ex;
                    }

                    switch (response.getCode()) {
                        case ResponseCode.SUCCESS:
                            success.incrementAndGet();
                            break;
                        case ResponseCode.EXPIRED_TERM:
                            // 可能出现多个 term 更大的情况, 取其中最大的一个用作 Candidate 的 term
                            maxTerm.set(Math.max(maxTerm.get(), response.getTerm()));
                            break;
                        case ResponseCode.INCONSISTENT_LEADER:
                            // leader 不一致, 可能是之前分区导致的, 或者是之前的 leader 挂了导致集群有了新的 leader
                            inconsistentLeader.compareAndSet(true, false);
                            break;
                        default:
                            break;
                    }

                    // 放到这里判断的目的是为了恢复之前已经被设置为网络异常但这次成功的节点
                    if (response.getCode() == ResponseCode.NETWORK_ERROR) {
                        memberState.getLiveNodes().put(entry.getKey(), false);
                    } else {
                        memberState.getLiveNodes().put(entry.getKey(), true);
                    }

                } catch (Throwable e) {
                    logger.error("Send heartbeat error ", e);
                } finally {
                    if (memberState.getNodes().size() == all.get()) {
                        countDownLatch.countDown();
                    }
                }
            });

            countDownLatch.await(config.getHeartBeatTimeIntervalMs(), TimeUnit.MILLISECONDS);

            if (inconsistentLeader.get()) {
                changeRoleToCandidate(memberState.getTerm());
            } else if (maxTerm.get() > memberState.getTerm()) {
                changeRoleToCandidate(maxTerm.get());
            } else if (success.get() > memberState.getNodes().size() / 2) {
                // 如果超过半数, 表示正常
                lastHeartBeatTime = System.currentTimeMillis();
            } else {
                changeRoleToCandidate(memberState.getTerm());
            }
        }
    }

    private void changeRoleToCandidate(long term) {
        memberState.setTerm(Math.max(term, memberState.getTerm()));
        memberState.setLeaderId(null);
        memberState.setRole(Role.CANDIDATE);
    }

    public void setLeaderElector(LeaderElector leaderElector) {
        this.leaderElector = leaderElector;
    }

    public void setClientProtocol(ClientProtocol clientProtocol) {
        this.clientProtocol = clientProtocol;
    }

    public void setMemberState(MemberState memberState) {
        this.memberState = memberState;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public void setServerProtocol(ServerProtocol serverProtocol) {
        this.serverProtocol = serverProtocol;
    }

}
