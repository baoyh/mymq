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

    private final Config config;

    private final RemotingClient remotingClient;

    private final RemotingServer remotingServer;

    private final MemberState memberState;

    public RaftServer(Config config, RemotingClient remotingClient, RemotingServer remotingServer) {
        this.config = config;
        this.remotingClient = remotingClient;
        this.remotingServer = remotingServer;

        this.memberState = createMemberState();
    }

    public void startup() {
        startStateMaintainer(memberState);
    }

    private void startStateMaintainer(MemberState memberState) {
        StateMaintainer stateMaintainer = new StateMaintainer();
        stateMaintainer.setMemberState(memberState);
        stateMaintainer.setConfig(config);
        stateMaintainer.setClientProtocol(new NettyClientProtocol(remotingClient, memberState));
        NettyServerProtocol serverProtocol = new NettyServerProtocol(memberState);
        stateMaintainer.setServerProtocol(serverProtocol);
        remotingServer.registerRequestProcessor(serverProtocol, RequestCode.SEND_HEARTBEAT, RequestCode.CALL_VOTE);
        stateMaintainer.setLeaderElector(new LeaderElector(memberState));
        stateMaintainer.start();
    }

    private MemberState createMemberState() {
        MemberState memberState = new MemberState();
        memberState.setConfig(config);
        memberState.setTerm(0);
        memberState.setSelfId(UUID.randomUUID().toString());
        return memberState;
    }

    public Config getConfig() {
        return config;
    }

    public MemberState getMemberState() {
        return memberState;
    }
}
