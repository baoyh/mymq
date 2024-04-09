package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.remoting.RemotingServer;

/**
 * @author baoyh
 * @since 2024/4/9 13:53
 */
public class NettyServerProtocol implements ServerProtocol {

    private final RemotingServer remotingServer;

    public NettyServerProtocol(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    @Override
    public void handleHeartbeats() {

    }
}
