package bao.study.mymq.remoting.netty;

import bao.study.mymq.remoting.common.RemotingCommand;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author baoyh
 * @since 2022/5/13 16:56
 */
public interface NettyRequestProcessor {

    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg);

    default void callback(ChannelFuture listener) {

    }
}
