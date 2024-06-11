package bao.study.mymq.broker;


import bao.study.mymq.broker.longpolling.PullRequestHoldService;
import bao.study.mymq.broker.manager.CommitLogManager;
import bao.study.mymq.broker.manager.ConsumeQueueIndexManager;
import bao.study.mymq.broker.manager.ConsumeQueueManager;
import bao.study.mymq.broker.processor.PullMessageProcessor;
import bao.study.mymq.broker.processor.SendMessageProcessor;
import bao.study.mymq.broker.raft.RaftServer;
import bao.study.mymq.broker.store.CommitLog;
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

    private final PullRequestHoldService pullRequestHoldService;

    private final HeartbeatService heartbeatService;

    private final RaftServer raftServer;

    public BrokerController(RemotingClient remotingClient, RemotingServer remotingServer, BrokerProperties brokerProperties, ConsumeQueueIndexManager consumeQueueIndexManager,
                            ConsumeQueueManager consumeQueueManager, CommitLogManager commitLogManager, RaftServer raftServer) {
        this.remotingClient = remotingClient;
        this.remotingServer = remotingServer;
        this.brokerProperties = brokerProperties;
        this.consumeQueueIndexManager = consumeQueueIndexManager;
        this.consumeQueueManager = consumeQueueManager;
        this.commitLogManager = commitLogManager;

        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.raftServer = raftServer;

        this.heartbeatService = new HeartbeatService(this, addressMap -> {
            Map<String, String> nodes = new HashMap<>();
            addressMap.forEach((k, v) -> nodes.put(brokerProperties.getBrokerName() + Constant.RAFT_ID_SEPARATOR + k, v));
            raftServer.updateNodes(nodes, brokerProperties.getBrokerName() + Constant.RAFT_ID_SEPARATOR + brokerProperties.getBrokerId());
        });
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

}
