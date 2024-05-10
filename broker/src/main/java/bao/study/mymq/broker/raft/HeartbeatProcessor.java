package bao.study.mymq.broker.raft;

import bao.study.mymq.broker.raft.protocol.ClientProtocol;
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
 * @author baoyh
 * @since 2024/5/7 9:27
 */
public class HeartbeatProcessor {

    private static final Logger logger = LoggerFactory.getLogger(StateMaintainer.class);

    private final Object lock = new Object();

    private final MemberState memberState;

    private final StateMaintainer stateMaintainer;

    private final ClientProtocol clientProtocol;

    public HeartbeatProcessor(StateMaintainer stateMaintainer, ClientProtocol clientProtocol) {
        this.memberState = stateMaintainer.getMemberState();
        this.stateMaintainer = stateMaintainer;
        this.clientProtocol = clientProtocol;
    }

    public void sendHeartBeats() throws InterruptedException {
        AtomicInteger all = new AtomicInteger(1);
        AtomicInteger success = new AtomicInteger(1);
        AtomicLong maxTerm = new AtomicLong(memberState.getTerm());
        AtomicBoolean inconsistentLeader = new AtomicBoolean(false);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        for (Map.Entry<String, String> entry : memberState.getNodes().entrySet()) {
            if (entry.getKey().equals(memberState.getSelfId())) {
                // 自己不需要发送给自己
                continue;
            }

            HeartBeat heartBeat = new HeartBeat();
            heartBeat.setCode(RequestCode.SEND_HEARTBEAT);
            heartBeat.setLeaderId(memberState.getSelfId());
            heartBeat.setRemoteId(entry.getKey());
            heartBeat.setLocalId(memberState.getSelfId());
            heartBeat.setTerm(memberState.getTerm());

            logger.info("{} start send heartbeat to {} at local term {}", memberState.getSelfId(), heartBeat.getRemoteId(), memberState.getTerm());
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
                            // 可能出现多个 term 更大的情况, 取其中最大的一个用作 candidate 的 term
                            maxTerm.set(Math.max(maxTerm.get(), response.getTerm()));
                            break;
                        case ResponseCode.INCONSISTENT_LEADER:
                            // leader 不一致, 可能是之前分区导致的, 或者是之前的 leader 挂了导致集群有了新的 leader
                            // 此时需要重新选举
                            inconsistentLeader.compareAndSet(true, false);
                            break;
                        case ResponseCode.TERM_NOT_READY:
                            // 远程节点的 term 小于当前 term 的情况
                            // 导致这种情况的原因一般是远程节点挂了后又重启
                            // 远程节点会更新 term, 依然作为 follower, 这里看做是成功. 在 dledger 中会变为 candidate
                            success.incrementAndGet();
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
        }

        countDownLatch.await(memberState.getConfig().getHeartBeatTimeIntervalMs(), TimeUnit.MILLISECONDS);

        if (inconsistentLeader.get()) {
            stateMaintainer.changeRoleToCandidate(memberState.getTerm());
        } else if (maxTerm.get() > memberState.getTerm()) {
            stateMaintainer.changeRoleToCandidate(maxTerm.get());
        } else if (success.get() > memberState.getNodes().size() / 2) {
            // 如果超过半数, 表示正常
            stateMaintainer.setLastHeartBeatTime(System.currentTimeMillis());
        } else {
            stateMaintainer.changeRoleToCandidate(memberState.getTerm());
        }
    }

    public HeartBeat handleHeartbeat(HeartBeat heartBeat) {
        logger.info("{} receive heartbeat from {} at local term {} and remote term {}", memberState.getSelfId(), heartBeat.getLocalId(), memberState.getTerm(), heartBeat.getTerm());
        MemberState memberState = stateMaintainer.getMemberState();
        stateMaintainer.setLastHeartBeatTime(System.currentTimeMillis());

        HeartBeat heartbeatResponse = createHeartbeatResponse(heartBeat);

        synchronized (lock) {
            if (heartBeat.getTerm() >= memberState.getTerm()) {
                stateMaintainer.changeRoleToFollower(heartBeat.getTerm());
                return heartbeatResponse;
            }

            if (heartBeat.getLeaderId().equals(memberState.getLeaderId())) {
                heartbeatResponse.setCode(ResponseCode.EXPIRED_TERM);
            } else if (memberState.getLeaderId() == null) {
                // 刚完成选举, 新 leader 第一次发送心跳
                memberState.setLeaderId(heartBeat.getLeaderId());
                memberState.setRole(Role.FOLLOWER);
                memberState.setTerm(heartBeat.getTerm());
            } else {
                // 由于分区后重新选举导致的 leader 不一致
                heartbeatResponse.setCode(ResponseCode.INCONSISTENT_LEADER);
            }
        }

        return heartbeatResponse;
    }

    private HeartBeat createHeartbeatResponse(HeartBeat heartBeat) {
        MemberState memberState = stateMaintainer.getMemberState();
        HeartBeat heartBeatResponse = new HeartBeat();
        heartBeatResponse.setTerm(memberState.getTerm());
        heartBeatResponse.setLocalId(memberState.getSelfId());
        heartBeatResponse.setRemoteId(heartBeat.getLocalId());
        heartBeatResponse.setLeaderId(memberState.getLeaderId());
        heartBeatResponse.setCode(ResponseCode.SUCCESS);
        return heartBeatResponse;
    }
}
