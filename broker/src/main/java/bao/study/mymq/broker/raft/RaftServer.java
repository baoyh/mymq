package bao.study.mymq.broker.raft;

import bao.study.mymq.broker.raft.protocol.NettyClientProtocol;
import bao.study.mymq.broker.raft.protocol.NettyServerProtocol;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;

import java.util.UUID;

/**
 * 服务入口
 *
 * @author baoyh
 * @since 2024/4/9 11:11
 */
public class RaftServer {

    private volatile boolean alive;

    private final Config config;

    private final RemotingClient remotingClient;

    private final RemotingServer remotingServer;

    private final MemberState memberState;

    private StateMaintainer stateMaintainer;

    public RaftServer(Config config, RemotingClient remotingClient, RemotingServer remotingServer) {
        this.config = config;
        this.remotingClient = remotingClient;
        this.remotingServer = remotingServer;

        this.memberState = createMemberState();
    }

    public void startup() {
        startStateMaintainer();
        alive = true;
    }

    private void startStateMaintainer() {
        synchronized (memberState) {
            if (stateMaintainer == null) {
                stateMaintainer = new StateMaintainer(memberState);
                NettyClientProtocol clientProtocol = new NettyClientProtocol(remotingClient, memberState);
                LeaderElector leaderElector = new LeaderElector(stateMaintainer, clientProtocol);
                HeartbeatProcessor heartbeatProcessor = new HeartbeatProcessor(stateMaintainer, clientProtocol);
                NettyServerProtocol serverProtocol = new NettyServerProtocol(heartbeatProcessor, leaderElector);
                remotingServer.registerRequestProcessor(serverProtocol, RequestCode.SEND_HEARTBEAT, RequestCode.CALL_VOTE);

                stateMaintainer.setHeartbeatProcessor(heartbeatProcessor);
                stateMaintainer.setLeaderElector(leaderElector);
            }
        }
        stateMaintainer.start();
    }

    private MemberState createMemberState() {
        MemberState memberState = new MemberState();
        memberState.setConfig(config);
        memberState.setTerm(0);
        memberState.setSelfId(UUID.randomUUID().toString());
        return memberState;
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        remotingServer.shutdown();
        remotingClient.shutdown();
        alive = false;
    }

    public Config getConfig() {
        return config;
    }

    public MemberState getMemberState() {
        return memberState;
    }

    public boolean isAlive() {
        return alive;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }
}

