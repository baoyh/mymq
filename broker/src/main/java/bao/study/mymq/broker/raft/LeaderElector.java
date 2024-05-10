package bao.study.mymq.broker.raft;

import bao.study.mymq.broker.raft.protocol.ClientProtocol;
import bao.study.mymq.common.protocol.raft.VoteRequest;
import bao.study.mymq.common.protocol.raft.VoteResponse;
import bao.study.mymq.remoting.code.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author baoyh
 * @since 2024/4/9 10:37
 */
public class LeaderElector {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElector.class);

    private static final Object lock = new Object();

    private final MemberState memberState;

    private final StateMaintainer stateMaintainer;

    private final ClientProtocol clientProtocol;

    public LeaderElector(StateMaintainer stateMaintainer, ClientProtocol clientProtocol) {
        this.memberState = stateMaintainer.getMemberState();
        this.stateMaintainer = stateMaintainer;
        this.clientProtocol = clientProtocol;
    }

    public VoteResult callVote() throws Exception {
        AtomicInteger success = new AtomicInteger(1);
        AtomicLong maxTerm = new AtomicLong(memberState.getTerm());
        maxTerm.incrementAndGet();
        memberState.setTerm(maxTerm.get());
        AtomicBoolean hasLeader = new AtomicBoolean(false);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // vote for self
        memberState.setCurrVoteFor(memberState.getSelfId());
        logger.info(memberState.getSelfId() + " voted for self at term " + maxTerm.get());

        for (String id : memberState.getNodes().keySet()) {

            if (id.equals(memberState.getSelfId())) {
                // 排除自身
                continue;
            }

            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setTerm(maxTerm.get());
            voteRequest.setRemoteId(id);
            voteRequest.setLocalId(memberState.getSelfId());

            logger.info(memberState.getSelfId() + " start call vote " + voteRequest.getRemoteId());
            CompletableFuture<VoteResponse> voteFeature = clientProtocol.callVote(voteRequest);

            voteFeature.whenComplete(((VoteResponse voteResponse, Throwable ex) -> {
                try {
                    if (ex != null) {
                        memberState.getLiveNodes().remove(id);
                        throw ex;
                    }
                    switch (voteResponse.getCode()) {
                        case ResponseCode.SUCCESS:
                            logger.info(voteResponse.getLocalId() + " voted " + memberState.getSelfId());
                            success.incrementAndGet();
                            break;
                        case ResponseCode.REJECT_ALREADY_VOTED:
                            logger.info(voteResponse.getLocalId() + " reject already voted " + memberState.getSelfId());
                            break;
                        case ResponseCode.REJECT_EXPIRED_TERM:
                            logger.info(voteResponse.getLocalId() + " reject expired term " + memberState.getSelfId());
                            maxTerm.set(Math.max(maxTerm.get(), voteResponse.getTerm()));
                            break;
                        case ResponseCode.REJECT_ALREADY_HAS_LEADER:
                            logger.info(voteResponse.getLocalId() + " reject already has leader " + memberState.getSelfId());
                            // term 相同但已经存在 leader, 一般是网络分区导致有过重新选举, 需将节点设置为 follower 等待接受心跳
                            hasLeader.compareAndSet(false, true);
                            break;
                        case ResponseCode.REJECT_TERM_NOT_READY:
                            logger.info(voteResponse.getLocalId() + " reject term not ready " + memberState.getSelfId());
                            // 远程节点 term 更小
                            break;
                    }
                } catch (Throwable e) {
                    logger.error(String.format("Vote error to node [%s]", id), e);
                } finally {
                    if (maxTerm.get() > memberState.getTerm() || success.get() > memberState.getNodes().size() / 2) {
                        countDownLatch.countDown();
                    }
                }
            }));
        }

        countDownLatch.await(memberState.getConfig().getMaxVoteIntervalMs(), TimeUnit.MILLISECONDS);

        if (hasLeader.get()) {
            stateMaintainer.changeRoleToFollower(memberState.getTerm());
            return VoteResult.CHANGE_TO_FOLLOWER;
        }
        if (maxTerm.get() > memberState.getTerm()) {
            memberState.setTerm(maxTerm.get());
            return VoteResult.WAIT_TO_REVOTE;
        }
        if (success.get() <= memberState.getNodes().size() / 2) {
            return VoteResult.WAIT_TO_REVOTE;
        }
        return VoteResult.PASSED;
    }

    public VoteResponse handleVote(VoteRequest voteRequest) {
        logger.info(memberState.getSelfId() + " receive call vote from " + voteRequest.getLocalId());
        logger.info(memberState.getSelfId() + " curr voted for " + memberState.getCurrVoteFor());

        VoteResponse voteResponse = createVoteResponse(voteRequest);
        if (memberState.getRole() == Role.LEADER) {
            voteResponse.setCode(ResponseCode.REJECT_ALREADY_HAS_LEADER);
            return voteResponse;
        }

        synchronized (lock) {
            if (memberState.getTerm() < voteRequest.getTerm()) {
                logger.info(memberState.getSelfId() + " will vote for " + voteRequest.getLocalId() + " in local term " + memberState.getTerm() + " and remote term " + voteRequest.getTerm());
                // 当前轮次小于发起方的轮次, 投票给发起方
                memberState.setCurrVoteFor(voteRequest.getLocalId());
                stateMaintainer.changeRoleToCandidate(voteRequest.getTerm());
                return voteResponse;
            }
            if (memberState.getTerm() > voteRequest.getTerm()) {
                voteResponse.setCode(ResponseCode.REJECT_EXPIRED_TERM);
                return voteResponse;
            }
            if (memberState.getLeaderId() != null) {
                voteResponse.setCode(ResponseCode.REJECT_ALREADY_HAS_LEADER);
                return voteResponse;
            }
            if (memberState.getCurrVoteFor() != null) {
                voteResponse.setCode(ResponseCode.REJECT_ALREADY_VOTED);
                return voteResponse;
            }
        }
        memberState.setCurrVoteFor(voteRequest.getLocalId());
        return voteResponse;
    }

    private VoteResponse createVoteResponse(VoteRequest voteRequest) {
        VoteResponse response = new VoteResponse();
        response.setTerm(memberState.getTerm());
        response.setLocalId(memberState.getSelfId());
        response.setRemoteId(voteRequest.getLocalId());
        response.setLeaderId(memberState.getLeaderId());
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    public enum VoteResult {
        CHANGE_TO_FOLLOWER,
        WAIT_TO_REVOTE,
        PASSED
    }
}