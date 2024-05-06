package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.broker.BrokerException;
import bao.study.mymq.broker.raft.MemberState;
import bao.study.mymq.common.protocol.raft.HeartBeat;
import bao.study.mymq.common.protocol.raft.VoteRequest;
import bao.study.mymq.common.protocol.raft.VoteResponse;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;

import java.util.concurrent.CompletableFuture;

/**
 * @author baoyh
 * @since 2024/4/7 17:58
 */
public class NettyClientProtocol implements ClientProtocol {

    private final RemotingClient client;

    private final MemberState memberState;

    public NettyClientProtocol(RemotingClient client, MemberState memberState) {
        this.client = client;
        this.memberState = memberState;
    }

    @Override
    public CompletableFuture<HeartBeat> sendHeartBeat(HeartBeat heartBeat) {
        CompletableFuture<HeartBeat> future = new CompletableFuture<>();
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.SEND_HEARTBEAT, CommonCodec.encode(heartBeat));
        client.invokeAsync(getAddress(heartBeat.getRemoteId()), remotingCommand, memberState.getConfig().getRpcTimeoutMillis(), responseFuture -> {
            RemotingCommand responseCommand = responseFuture.getResponseCommand();
            if (responseCommand == null) {
                HeartBeat heartBeatResponse = new HeartBeat();
                heartBeatResponse.setCode(ResponseCode.NETWORK_ERROR);
                future.complete(heartBeatResponse);
            } else {
                future.complete(CommonCodec.decode(responseCommand.getBody(), HeartBeat.class));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<VoteResponse> callVote(VoteRequest voteRequest) {
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.CALL_VOTE, CommonCodec.encode(voteRequest));
        client.invokeAsync(getAddress(voteRequest.getRemoteId()), remotingCommand, memberState.getConfig().getRpcTimeoutMillis(), responseFuture -> {
            RemotingCommand responseCommand = responseFuture.getResponseCommand();
            if (responseCommand == null) {
                VoteResponse voteResponse = new VoteResponse();
                voteResponse.setCode(ResponseCode.NETWORK_ERROR);
                future.complete(voteResponse);
            } else {
                future.complete(CommonCodec.decode(responseCommand.getBody(), VoteResponse.class));
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
