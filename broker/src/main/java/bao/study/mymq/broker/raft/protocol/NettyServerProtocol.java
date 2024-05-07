package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.broker.raft.*;
import bao.study.mymq.common.protocol.raft.HeartBeat;
import bao.study.mymq.common.protocol.raft.VoteRequest;
import bao.study.mymq.common.protocol.raft.VoteResponse;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;

import static bao.study.mymq.remoting.code.RequestCode.*;

/**
 * @author baoyh
 * @since 2024/4/9 13:53
 */
public class NettyServerProtocol implements ServerProtocol, NettyRequestProcessor {

    private final HeartbeatProcessor heartbeatProcessor;

    private final LeaderElector leaderElector;

    public NettyServerProtocol(HeartbeatProcessor heartbeatProcessor, LeaderElector leaderElector) {
        this.heartbeatProcessor = heartbeatProcessor;
        this.leaderElector = leaderElector;
    }

    @Override
    public HeartBeat handleHeartbeat(HeartBeat heartBeat) {
        return heartbeatProcessor.handleHeartbeat(heartBeat);
    }

    @Override
    public VoteResponse handleVote(VoteRequest voteRequest) {
        return leaderElector.handleVote(voteRequest);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        switch (msg.getCode()) {
            case SEND_HEARTBEAT:
                HeartBeat response = handleHeartbeat(CommonCodec.decode(msg.getBody(), HeartBeat.class));
                return RemotingCommandFactory.createResponseRemotingCommand(response.getCode(), CommonCodec.encode(response));
            case CALL_VOTE:
                VoteResponse voteResponse = handleVote(CommonCodec.decode(msg.getBody(), VoteRequest.class));
                return RemotingCommandFactory.createResponseRemotingCommand(voteResponse.getCode(), CommonCodec.encode(voteResponse));
            default:
                return null;
        }
    }
}
