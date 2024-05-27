package bao.study.mymq.broker.raft;

import bao.study.mymq.broker.raft.protocol.NettyClientProtocol;
import bao.study.mymq.broker.raft.protocol.NettyServerProtocol;
import bao.study.mymq.broker.raft.store.RaftFileStore;
import bao.study.mymq.broker.raft.store.RaftStore;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.code.RequestCode;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 服务入口
 *
 * @author baoyh
 * @since 2024/4/9 11:11
 */
public class RaftServer {

    private static final Object lock = new Object();

    private AtomicBoolean alive = new AtomicBoolean(false);

    private final Config config;

    private final RemotingClient remotingClient;

    private final RemotingServer remotingServer;

    private final MemberState memberState;

    private StateMaintainer stateMaintainer;

    private final RaftStore raftStore;

    private final EntryProcessor entryProcessor;

    public RaftServer(Config config, RemotingClient remotingClient, RemotingServer remotingServer) {
        this.config = config;
        this.remotingClient = remotingClient;
        this.remotingServer = remotingServer;

        this.memberState = createMemberState();
        this.raftStore = new RaftFileStore(config);
        this.entryProcessor = new EntryProcessor(memberState, new NettyClientProtocol(remotingClient, memberState), raftStore);
    }

    public void startup() {
        startRemoting();
        startStateMaintainer();
        startEntryProcessor();
        alive.compareAndSet(false, true);
    }

    private void startRemoting() {
        remotingServer.start();
        remotingClient.start();
    }

    private void startStateMaintainer() {
        synchronized (lock) {
            if (stateMaintainer == null) {
                stateMaintainer = new StateMaintainer(memberState);
                NettyClientProtocol clientProtocol = new NettyClientProtocol(remotingClient, memberState);
                LeaderElector leaderElector = new LeaderElector(stateMaintainer, clientProtocol);
                HeartbeatProcessor heartbeatProcessor = new HeartbeatProcessor(stateMaintainer, clientProtocol);
                NettyServerProtocol serverProtocol = new NettyServerProtocol(heartbeatProcessor, leaderElector, entryProcessor);
                remotingServer.registerRequestProcessor(serverProtocol, RequestCode.SEND_HEARTBEAT, RequestCode.CALL_VOTE, RequestCode.APPEND, RequestCode.PUSH);

                stateMaintainer.setHeartbeatProcessor(heartbeatProcessor);
                stateMaintainer.setLeaderElector(leaderElector);
            }
        }
        stateMaintainer.start();
    }

    private void startEntryProcessor() {
        entryProcessor.start();
    }

    private MemberState createMemberState() {
        MemberState memberState = new MemberState();
        memberState.setConfig(config);
        memberState.setTerm(0);
        memberState.setSelfId(UUID.randomUUID().toString());
        config.setSelfId(memberState.getSelfId());
        return memberState;
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        stateMaintainer = null;
        remotingServer.shutdown();
        remotingClient.shutdown();
        alive.compareAndSet(true, false);
    }

    public Config getConfig() {
        return config;
    }

    public MemberState getMemberState() {
        return memberState;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public boolean getAlive() {
        return alive.get();
    }
}

