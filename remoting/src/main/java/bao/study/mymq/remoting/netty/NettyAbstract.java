package bao.study.mymq.remoting.netty;

import bao.study.mymq.remoting.common.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

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

    public ChannelPipeline objectCodec(SocketChannel ch, ClassLoader classLoader) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new ObjectDecoder(1024 * 1024, ClassResolvers.weakCachingConcurrentResolver(classLoader)));
        pipeline.addLast(new ObjectEncoder());
        return pipeline;
    }
}
