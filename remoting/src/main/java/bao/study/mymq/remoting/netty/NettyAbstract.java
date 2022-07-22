package bao.study.mymq.remoting.netty;

import bao.study.mymq.remoting.common.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2022/5/13 16:59
 */
public abstract class NettyAbstract {

    private final Map<Integer, NettyRequestProcessor> requestProcessorMap = new ConcurrentHashMap<>();

    public void registerRequestProcessor(int code, NettyRequestProcessor requestProcessor) {
        requestProcessorMap.putIfAbsent(code, requestProcessor);
    }

    public void processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        NettyRequestProcessor requestProcessor = requestProcessorMap.get(msg.getCode());
        if (requestProcessor != null) {
            requestProcessor.processRequest(ctx, msg);
        }
    }
}
