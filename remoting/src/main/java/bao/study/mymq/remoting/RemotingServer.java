package bao.study.mymq.remoting;

import bao.study.mymq.remoting.netty.NettyRequestProcessor;

/**
 * @author baoyh
 * @since 2022/5/13 15:14
 */
public interface RemotingServer extends Remoting {

    void registerRequestProcessor(int code, NettyRequestProcessor requestProcessor);
}
