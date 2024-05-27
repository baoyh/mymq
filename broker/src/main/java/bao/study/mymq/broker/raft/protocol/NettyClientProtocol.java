package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.broker.BrokerException;
import bao.study.mymq.broker.raft.MemberState;
import bao.study.mymq.common.protocol.raft.*;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author baoyh
 * @since 2024/4/7 17:58
 */
public class NettyClientProtocol implements ClientProtocol {

    private static final Logger logger = LoggerFactory.getLogger(NettyClientProtocol.class);

    private final RemotingClient client;

    private final MemberState memberState;

    private ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {

        private AtomicInteger threadIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "RaftExecutor_" + this.threadIndex.incrementAndGet());
        }
    });


    public NettyClientProtocol(RemotingClient client, MemberState memberState) {
        this.client = client;
        this.memberState = memberState;
    }

    @Override
    public CompletableFuture<HeartBeat> sendHeartBeat(HeartBeat heartBeat) {
        CompletableFuture<HeartBeat> future = new CompletableFuture<>();
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.SEND_HEARTBEAT, CommonCodec.encode(heartBeat));

        HeartBeat errorResponse = new HeartBeat();
        errorResponse.setCode(ResponseCode.NETWORK_ERROR);

        executor.execute(() -> {
            try {
                client.invokeAsync(getAddress(heartBeat.getRemoteId()), remotingCommand, memberState.getConfig().getRpcTimeoutMillis(), responseFuture -> {
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand == null) {
                        future.complete(errorResponse);
                    } else {
                        future.complete(CommonCodec.decode(responseCommand.getBody(), HeartBeat.class));
                    }
                });
            } catch (Throwable e) {
                logger.error("Send heartBeat request failed, {}, because {}", heartBeat.baseInfo(), e.getMessage());
                future.complete(errorResponse);
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<VoteResponse> callVote(VoteRequest voteRequest) {
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.CALL_VOTE, CommonCodec.encode(voteRequest));

        VoteResponse errorResponse = new VoteResponse();
        errorResponse.setCode(ResponseCode.NETWORK_ERROR);

        executor.execute(() -> {
            try {
                client.invokeAsync(getAddress(voteRequest.getRemoteId()), remotingCommand, memberState.getConfig().getRpcTimeoutMillis(), responseFuture -> {
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand == null) {
                        future.complete(errorResponse);
                    } else {
                        future.complete(CommonCodec.decode(responseCommand.getBody(), VoteResponse.class));
                    }
                });
            } catch (Throwable e) {
                logger.error("Call vote request failed, {}, because {}", voteRequest.baseInfo(), e.getMessage());
                future.complete(errorResponse);
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<PushEntryResponse> push(PushEntryRequest entryRequest) {
        CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.PUSH, CommonCodec.encode(entryRequest));

        PushEntryResponse errorResponse = new PushEntryResponse();
        errorResponse.setCode(ResponseCode.NETWORK_ERROR);

        executor.execute(() -> {
            try {
                client.invokeAsync(getAddress(entryRequest.getRemoteId()), remotingCommand, memberState.getConfig().getRpcTimeoutMillis(), responseFuture -> {
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand == null) {
                        future.complete(errorResponse);
                    } else {
                        future.complete(CommonCodec.decode(responseCommand.getBody(), PushEntryResponse.class));
                    }
                });
            } catch (Throwable e) {
                logger.error("Append request failed, {}, because {}", entryRequest.baseInfo(), e.getMessage());
                future.complete(errorResponse);
            }
        });

        return future;
    }

    private String getAddress(String remoteId) {
        String address = memberState.getNodes().get(remoteId);
        if (address == null || address.isEmpty()) {
            throw new BrokerException("Can not found remote address by remoteId [" + remoteId + "]");
        }
        return address;
    }
}
