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

    private final AtomicBoolean alive = new AtomicBoolean(false);

    private final Config config;

    private final RemotingClient remotingClient;

    private final RemotingServer remotingServer;

    private final MemberState memberState;

    private StateMaintainer stateMaintainer;

    private RaftStore raftStore;

    private EntryProcessor entryProcessor;

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
        startRaftStore();
        startEntryProcessor();
        startStateMaintainer();
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

                if (memberState.getLeaderId() != null) {
                    stateMaintainer.setLastHeartBeatTime(System.currentTimeMillis());
                }
            }
        }
        stateMaintainer.start();
    }

    private void startEntryProcessor() {
        synchronized (lock) {
            if (entryProcessor == null) {
                entryProcessor = new EntryProcessor(memberState, new NettyClientProtocol(remotingClient, memberState), raftStore);
            }
        }
        entryProcessor.start();
    }

    private void startRaftStore() {
        synchronized (lock) {
            if (raftStore == null) {
                raftStore = new RaftFileStore(config);
            }
        }
        raftStore.startup();
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
        remotingServer.shutdown();
        remotingClient.shutdown();
        raftStore.shutdown();
        entryProcessor.shutdown();
        alive.compareAndSet(true, false);
        reset();
    }

    private void reset() {
        stateMaintainer = null;
        raftStore = null;
        entryProcessor = null;
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

