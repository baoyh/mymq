package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.common.protocol.raft.*;

import java.util.concurrent.CompletableFuture;

/**
 * @author baoyh
 * @since 2024/4/7 17:42
 */
public interface ClientProtocol {

    CompletableFuture<HeartBeat> sendHeartBeat(HeartBeat heartBeat);

    CompletableFuture<VoteResponse> callVote(VoteRequest voteRequest);

    CompletableFuture<PushEntryResponse> push(PushEntryRequest entryRequest);
}
