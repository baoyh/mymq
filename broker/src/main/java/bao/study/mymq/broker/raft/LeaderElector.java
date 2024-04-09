package bao.study.mymq.broker.raft;

import bao.study.mymq.broker.raft.protocol.ClientProtocol;
import bao.study.mymq.common.protocol.raft.VoteRequest;
import bao.study.mymq.common.protocol.raft.VoteResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
        Map<String, String> nodes = memberState.getNodes();
        nodes.forEach((id, address) -> {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setTerm(memberState.getTerm());
            voteRequest.setRemoteId(id);
            voteRequest.setLocalId(memberState.getSelfId());
            CompletableFuture<VoteResponse> voteFeature = clientProtocol.callVote(voteRequest);

            voteFeature.whenComplete(((VoteResponse voteResponse, Throwable ex) -> {

            }));
        });
    }
}
