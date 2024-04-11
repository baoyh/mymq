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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author baoyh
 * @since 2024/4/9 10:37
 */
public class LeaderElector {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElector.class);

    private final MemberState memberState;

    private final ClientProtocol clientProtocol;

    public LeaderElector(MemberState memberState, ClientProtocol clientProtocol) {
        this.memberState = memberState;
        this.clientProtocol = clientProtocol;
    }

    public VoteResult callVote() throws Exception {
        AtomicInteger success = new AtomicInteger(1);
        AtomicLong maxTerm = new AtomicLong(memberState.getTerm());

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // vote for self
        memberState.setLeaderId(memberState.getSelfId());

        for (String id : memberState.getNodes().keySet()) {

            if (id.equals(memberState.getSelfId())) {
                // 排除自身
                continue;
            }

            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setTerm(memberState.getTerm());
            voteRequest.setRemoteId(id);
            voteRequest.setLocalId(memberState.getSelfId());
            CompletableFuture<VoteResponse> voteFeature = clientProtocol.callVote(voteRequest);

            voteFeature.whenComplete(((VoteResponse voteResponse, Throwable ex) -> {
                try {
                    if (ex != null) {
                        memberState.getLiveNodes().remove(id);
                        throw ex;
                    }
                    switch (voteResponse.getCode()) {
                        case ResponseCode.SUCCESS:
                            success.incrementAndGet();
                            break;
                        case ResponseCode.REJECT_ALREADY_VOTED:
                            break;
                        case ResponseCode.REJECT_EXPIRED_TERM:
                            maxTerm.set(Math.max(maxTerm.get(), voteResponse.getTerm()));
                            break;
                        case ResponseCode.REJECT_ALREADY_HAS_LEADER:
                            // term 相同但已经存在 leader, 一般是网络分区导致有过重新选举
                            break;
                        case ResponseCode.REJECT_TERM_NOT_READY:
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

        if (maxTerm.get() > memberState.getTerm()) {
            memberState.setTerm(maxTerm.get());
            return VoteResult.REVOTE_IMMEDIATELY;
        }
        if (success.get() <= memberState.getNodes().size() / 2) {
            return VoteResult.WAIT_TO_REVOTE;
        }
        return VoteResult.PASSED;
    }

    public enum VoteResult {
        WAIT_TO_REVOTE,
        REVOTE_IMMEDIATELY,
        PASSED
    }
}