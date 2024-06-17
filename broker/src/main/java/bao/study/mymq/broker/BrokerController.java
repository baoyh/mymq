package bao.study.mymq.broker;


import bao.study.mymq.broker.config.ConsumeQueueConfig;
import bao.study.mymq.broker.config.MessageStoreConfig;
import bao.study.mymq.broker.longpolling.PullRequestHoldService;
import bao.study.mymq.broker.manager.CommitLogManager;
import bao.study.mymq.broker.manager.ConsumeQueueIndexManager;
import bao.study.mymq.broker.manager.ConsumeQueueManager;
import bao.study.mymq.broker.processor.FlushSyncProcessor;
import bao.study.mymq.broker.processor.PullMessageProcessor;
import bao.study.mymq.broker.processor.SendMessageProcessor;
import bao.study.mymq.broker.raft.Config;
import bao.study.mymq.broker.raft.RaftServer;
import bao.study.mymq.broker.service.FlushSyncService;
import bao.study.mymq.broker.service.HeartbeatService;
import bao.study.mymq.broker.service.MasterManagerService;
import bao.study.mymq.broker.store.CommitLog;
import bao.study.mymq.broker.store.RaftCommitLog;
import bao.study.mymq.common.Constant;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingServer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author baoyh
 * @since 2022/10/14 10:28
 */
public class BrokerController {

    private final RemotingClient remotingClient;

    private final RemotingServer remotingServer;

    private final BrokerProperties brokerProperties;

    private final ConsumeQueueIndexManager consumeQueueIndexManager;

    private final ConsumeQueueManager consumeQueueManager;

    private final CommitLogManager commitLogManager;

    private final SendMessageProcessor sendMessageProcessor;

    private final PullMessageProcessor pullMessageProcessor;

    private final FlushSyncProcessor flushSyncProcessor;

    private final PullRequestHoldService pullRequestHoldService;

    private final HeartbeatService heartbeatService;

    private final MasterManagerService masterManagerService;

    private final FlushSyncService flushSyncService;

    private final RaftServer raftServer;

    public BrokerController(RemotingClient remotingClient, RemotingServer remotingServer, BrokerProperties brokerProperties) {
        this.remotingClient = remotingClient;
        this.remotingServer = remotingServer;
        this.brokerProperties = brokerProperties;

        this.consumeQueueIndexManager = new ConsumeQueueIndexManager(brokerProperties);
        this.consumeQueueManager = new ConsumeQueueManager(new ConsumeQueueConfig(), brokerProperties);

        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.masterManagerService = new MasterManagerService(this);

        this.raftServer = new RaftServer(new Config(), new HashMap<>(), getSelfId(), remotingClient, remotingServer, masterManagerService::registerMaster);
        this.commitLogManager = new CommitLogManager(new RaftCommitLog(new MessageStoreConfig(), raftServer), brokerProperties);

        this.heartbeatService = new HeartbeatService(this, (addressMap, leaderId) -> {
            Map<String, String> nodes = new HashMap<>();
            addressMap.forEach((k, v) -> nodes.put(brokerProperties.getBrokerName() + Constant.RAFT_ID_SEPARATOR + k, v));
            raftServer.updateNodes(nodes, brokerProperties.getBrokerName() + Constant.RAFT_ID_SEPARATOR + brokerProperties.getBrokerId(), leaderId);
        });
        this.flushSyncService = new FlushSyncService(this);
        this.flushSyncProcessor = new FlushSyncProcessor(this);
    }

    protected void initialize() {
        consumeQueueIndexManager.load();
        consumeQueueManager.load();
        commitLogManager.load();
    }

    protected void start() {
        pullRequestHoldService.start();
        raftServer.startup();
        heartbeatService.start();
    }

    public void shutdown() {
        remotingServer.shutdown();
        remotingClient.shutdown();
        pullRequestHoldService.shutdown();
        raftServer.shutdown();
        heartbeatService.shutdown();
    }


    public ConsumeQueueIndexManager getConsumeOffsetManager() {
        return consumeQueueIndexManager;
    }

    public ConsumeQueueManager getConsumeQueueManager() {
        return consumeQueueManager;
    }

    public CommitLogManager getCommitLogManager() {
        return commitLogManager;
    }

    public CommitLog getCommitLog() {
        return commitLogManager.getCommitLog();
    }

    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public BrokerProperties getBrokerProperties() {
        return brokerProperties;
    }

    public SendMessageProcessor getSendMessageProcessor() {
        return sendMessageProcessor;
    }

    public HeartbeatService getHeartbeatService() {
        return heartbeatService;
    }

    public FlushSyncService getFlushSyncService() {
        return flushSyncService;
    }

    public FlushSyncProcessor getFlushSyncProcessor() {
        return flushSyncProcessor;
    }

    private String getSelfId() {
        return brokerProperties.getBrokerName() + Constant.RAFT_ID_SEPARATOR + brokerProperties.getBrokerId();
    }

}
