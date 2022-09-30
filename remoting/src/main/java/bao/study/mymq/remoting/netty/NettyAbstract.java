package bao.study.mymq.remoting.netty;

import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.InvokeCallback;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandType;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2022/5/13 16:59
 */
public abstract class NettyAbstract {

    private final Map<Integer, NettyRequestProcessor> requestProcessorTable = new ConcurrentHashMap<>();

    protected final Map<Integer, ResponseFuture> responseFutureTable = new ConcurrentHashMap<>();

    public void registerRequestProcessor(int code, NettyRequestProcessor requestProcessor) {
        requestProcessorTable.putIfAbsent(code, requestProcessor);
    }

    public void processRemotingCommand(ChannelHandlerContext ctx, RemotingCommand msg) {
        switch (msg.getRemotingCommandType()) {
            case REQUEST:
                processRequest(ctx, msg);
                break;
            case RESPONSE:
                processResponse(ctx, msg);
                break;
            default:
                break;
        }
    }

    public void processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        NettyRequestProcessor requestProcessor = requestProcessorTable.get(msg.getCode());
        if (requestProcessor == null) return;

        RemotingCommand response = requestProcessor.processRequest(ctx, msg);
        response.setRequestId(msg.getRequestId());
        response.setRemotingCommandType(RemotingCommandType.RESPONSE);
        ctx.writeAndFlush(response);
    }

    public void processResponse(ChannelHandlerContext ctx, RemotingCommand msg) {
        ResponseFuture responseFuture = responseFutureTable.get(msg.getRequestId());
        if (responseFuture == null) return;

        responseFuture.setResponseCommand(msg);

        if (msg.getCode() != ResponseCode.SUCCESS) {
            responseFuture.setException(CommonCodec.decode(msg.getBody(), RuntimeException.class));
        }

        InvokeCallback invokeCallback = responseFuture.getInvokeCallback();
        if (invokeCallback != null) {
            invokeCallback.callback(responseFuture);
        }

        responseFuture.getCountDownLatch().countDown();
    }
}
