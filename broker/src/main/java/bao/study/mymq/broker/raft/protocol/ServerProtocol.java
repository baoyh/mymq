package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.common.protocol.raft.*;

import java.util.concurrent.CompletableFuture;

/**
 * @author baoyh
 * @since 2024/4/9 13:51
 */
public interface ServerProtocol {

    HeartBeat handleHeartbeat(HeartBeat heartBeat);

    VoteResponse handleVote(VoteRequest voteRequest);

    CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest entryRequest);

    CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest entryRequest);
}
