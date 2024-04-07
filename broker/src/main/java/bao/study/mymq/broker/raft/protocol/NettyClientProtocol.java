package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.broker.BrokerException;
import bao.study.mymq.broker.raft.MemberState;
import bao.study.mymq.common.protocol.raft.HeartBeat;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;

import java.util.concurrent.CompletableFuture;

/**
 * @author baoyh
 * @since 2024/4/7 17:58
 */
public class NettyClientProtocol implements ClientProtocol {

    private RemotingClient client;

    private MemberState memberState;

    @Override
    public CompletableFuture<HeartBeat> sendHeartBeat(HeartBeat heartBeat) {
        String address = memberState.getNodes().get(heartBeat.getRemoteId());
        if (address == null || address.isBlank()) {
            throw new BrokerException("Can not found remote address by remoteId [" + heartBeat.getRemoteId() + "]");
        }

        CompletableFuture<HeartBeat> future = new CompletableFuture<>();
        RemotingCommand remotingCommand = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.SEND_HEARTBEAT, CommonCodec.encode(heartBeat));
        client.invokeAsync(address, remotingCommand, memberState.getConfig().getRpcTimeoutMillis(), responseFuture -> {
            RemotingCommand responseCommand = responseFuture.getResponseCommand();
            if (responseCommand == null) {

            }
        });
        return null;
    }
}
