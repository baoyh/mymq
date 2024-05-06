package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.broker.raft.MemberState;
import bao.study.mymq.broker.raft.Role;
import bao.study.mymq.broker.raft.StateMaintainer;
import bao.study.mymq.common.protocol.raft.HeartBeat;
import bao.study.mymq.common.protocol.raft.VoteRequest;
import bao.study.mymq.common.protocol.raft.VoteResponse;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static bao.study.mymq.remoting.code.RequestCode.*;

/**
 * @author baoyh
 * @since 2024/4/9 13:53
 */
public class NettyServerProtocol implements ServerProtocol, NettyRequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(NettyServerProtocol.class);

    private final StateMaintainer stateMaintainer;

    public NettyServerProtocol(StateMaintainer stateMaintainer) {
        this.stateMaintainer = stateMaintainer;
    }

    @Override
    public HeartBeat handleHeartbeat(HeartBeat heartBeat) {
        MemberState memberState = stateMaintainer.getMemberState();
        stateMaintainer.setLastHeartBeatTime(System.currentTimeMillis());

        HeartBeat heartbeatResponse = createHeartbeatResponse(heartBeat);

        if (heartBeat.getLeaderId().equals(memberState.getLeaderId())) {
            if (heartBeat.getTerm() == memberState.getTerm()) {
                heartbeatResponse.setCode(ResponseCode.SUCCESS);
                return heartbeatResponse;
            }
            if (heartBeat.getTerm() > memberState.getTerm()) {
                memberState.setTerm(heartBeat.getTerm());
                heartbeatResponse.setCode(ResponseCode.TERM_NOT_READY);
                return heartbeatResponse;
            }
            heartbeatResponse.setCode(ResponseCode.EXPIRED_TERM);
        } else if (memberState.getLeaderId() == null) {
            // 刚完成选举, 新 leader 第一次发送心跳
            memberState.setLeaderId(heartBeat.getLeaderId());
            memberState.setRole(Role.FOLLOWER);
            memberState.setTerm(heartBeat.getTerm());
        } else {
            // 由于分区后重新选举导致的 leader 不一致
            heartbeatResponse.setCode(ResponseCode.INCONSISTENT_LEADER);
        }
        return heartbeatResponse;
    }

    @Override
    public VoteResponse handleVote(VoteRequest voteRequest) {
        MemberState memberState = stateMaintainer.getMemberState();
        VoteResponse voteResponse = createVoteResponse(voteRequest);
        if (memberState.getRole() == Role.LEADER) {
            voteResponse.setCode(ResponseCode.REJECT_ALREADY_HAS_LEADER);
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
        if (memberState.getTerm() > voteRequest.getTerm()) {
            voteResponse.setCode(ResponseCode.REJECT_EXPIRED_TERM);
            return voteResponse;
        }
        if (memberState.getTerm() < voteRequest.getTerm()) {
            if (memberState.getCurrVoteFor() == null || memberState.getCurrVoteFor().equals(memberState.getSelfId())) {
                logger.info(memberState.getSelfId() + " curr voted for " + memberState.getCurrVoteFor());
                // 当前轮次小于发起方的轮次, 投票给发起方
                memberState.setCurrVoteFor(voteRequest.getLocalId());
                logger.info(memberState.getSelfId() + " will vote for " + voteRequest.getLocalId() + " in local term " + memberState.getTerm() + " and remote term " + voteRequest.getTerm());
            } else {
                logger.info(memberState.getSelfId() + " reject already voted " + voteRequest.getLocalId() + " in local term " + memberState.getTerm() + " and remote term " + voteRequest.getTerm());
                voteResponse.setCode(ResponseCode.REJECT_ALREADY_VOTED);
            }
            return voteResponse;
        }
        memberState.setCurrVoteFor(voteRequest.getLocalId());
        return voteResponse;
    }

    private HeartBeat createHeartbeatResponse(HeartBeat heartBeat) {
        MemberState memberState = stateMaintainer.getMemberState();
        HeartBeat heartBeatResponse = new HeartBeat();
        heartBeatResponse.setTerm(memberState.getTerm());
        heartBeatResponse.setLocalId(memberState.getSelfId());
        heartBeatResponse.setRemoteId(heartBeat.getLocalId());
        heartBeatResponse.setLeaderId(memberState.getLeaderId());
        heartBeatResponse.setCode(ResponseCode.SUCCESS);
        return heartBeatResponse;
    }

    private VoteResponse createVoteResponse(VoteRequest voteRequest) {
        MemberState memberState = stateMaintainer.getMemberState();
        VoteResponse response = new VoteResponse();
        response.setTerm(memberState.getTerm());
        response.setLocalId(memberState.getSelfId());
        response.setRemoteId(voteRequest.getLocalId());
        response.setLeaderId(memberState.getLeaderId());
        response.setCode(ResponseCode.SUCCESS);
        return response;
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
