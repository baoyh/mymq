package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.broker.raft.*;
import bao.study.mymq.common.protocol.raft.*;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static bao.study.mymq.remoting.code.RequestCode.*;

/**
 * @author baoyh
 * @since 2024/4/9 13:53
 */
public class NettyServerProtocol implements ServerProtocol, NettyRequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(NettyServerProtocol.class);

    private final HeartbeatProcessor heartbeatProcessor;

    private final LeaderElector leaderElector;

    private final EntryProcessor entryProcessor;

    private ExecutorService futureExecutor = Executors.newFixedThreadPool(4);

    public NettyServerProtocol(HeartbeatProcessor heartbeatProcessor, LeaderElector leaderElector, EntryProcessor entryProcessor) {
        this.heartbeatProcessor = heartbeatProcessor;
        this.leaderElector = leaderElector;
        this.entryProcessor = entryProcessor;
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
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest entryRequest) {
        return entryProcessor.handleAppend(entryRequest);
    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest entryRequest) {
        return entryProcessor.handlePush(entryRequest);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        switch (msg.getCode()) {
            case SEND_HEARTBEAT:
                HeartBeat heartBeat = handleHeartbeat(CommonCodec.decode(msg.getBody(), HeartBeat.class));
                return RemotingCommandFactory.createResponseRemotingCommand(heartBeat.getCode(), CommonCodec.encode(heartBeat));
            case CALL_VOTE:
                VoteResponse voteResponse = handleVote(CommonCodec.decode(msg.getBody(), VoteRequest.class));
                return RemotingCommandFactory.createResponseRemotingCommand(voteResponse.getCode(), CommonCodec.encode(voteResponse));
            case APPEND:
                CompletableFuture<AppendEntryResponse> entryResponse = handleAppend(CommonCodec.decode(msg.getBody(), AppendEntryRequest.class));
                // 先返回 null, 等待方法完成后再向 client 返回结果, 好处是请求可以立马返回, 不阻塞 netty 的线程池
                entryResponse.whenCompleteAsync((response, ex) -> writeResponse(response, ex, msg, ctx), futureExecutor);
                break;
            case PUSH:
                CompletableFuture<PushEntryResponse> pushEntryResponse = handlePush(CommonCodec.decode(msg.getBody(), PushEntryRequest.class));
                pushEntryResponse.whenCompleteAsync((response, ex) -> writeResponse(response, ex, msg, ctx), futureExecutor);
                break;
        }
        return null;
    }

    private void writeResponse(BaseProtocol response, Throwable ex, RemotingCommand msg, ChannelHandlerContext ctx) {
        try {
            if (ex != null) {
                throw ex;
            }
            ctx.writeAndFlush(createResponse(response, msg));
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

    public RemotingCommand createResponse(BaseProtocol response, RemotingCommand request) {
        RemotingCommand remotingCommand = RemotingCommandFactory.createResponseRemotingCommand(ResponseCode.SUCCESS, CommonCodec.encode(response));
        remotingCommand.setRequestId(request.getRequestId());
        return remotingCommand;
    }
}
