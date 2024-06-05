package bao.study.mymq.broker;


import bao.study.mymq.broker.longpolling.PullRequestHoldService;
import bao.study.mymq.broker.manager.CommitLogManager;
import bao.study.mymq.broker.manager.ConsumeQueueIndexManager;
import bao.study.mymq.broker.manager.ConsumeQueueManager;
import bao.study.mymq.broker.processor.PullMessageProcessor;
import bao.study.mymq.broker.store.CommitLog;

/**
 * @author baoyh
 * @since 2022/10/14 10:28
 */
public class BrokerController {

    private final ConsumeQueueIndexManager consumeQueueIndexManager;

    private final ConsumeQueueManager consumeQueueManager;

    private final CommitLogManager commitLogManager;

    private final PullMessageProcessor pullMessageProcessor;

    private final PullRequestHoldService pullRequestHoldService;

    public BrokerController(ConsumeQueueIndexManager consumeQueueIndexManager, ConsumeQueueManager consumeQueueManager, CommitLogManager commitLogManager) {
        this.consumeQueueIndexManager = consumeQueueIndexManager;
        this.consumeQueueManager = consumeQueueManager;
        this.commitLogManager = commitLogManager;

        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
    }

    protected void initialize() {
        consumeQueueIndexManager.load();
        consumeQueueManager.load();
        commitLogManager.load();
    }

    protected void start() {
        pullRequestHoldService.start();
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
}
