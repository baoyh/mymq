package bao.study.mymq.broker.raft;

import bao.study.mymq.broker.raft.protocol.ClientProtocol;
import bao.study.mymq.common.protocol.raft.VoteRequest;
import bao.study.mymq.common.protocol.raft.VoteResponse;
import bao.study.mymq.remoting.code.ResponseCode;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author baoyh
 * @since 2024/4/9 10:37
 */
public class LeaderElector {

    private MemberState memberState;

    private ClientProtocol clientProtocol;

    public LeaderElector(MemberState memberState) {
        this.memberState = memberState;
    }

    public void callVote() {
        AtomicInteger success = new AtomicInteger();
        AtomicLong maxTerm = new AtomicLong(memberState.getTerm());

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

                }
            }));
        }
    }
}
