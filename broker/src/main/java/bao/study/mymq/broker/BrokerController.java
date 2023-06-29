package bao.study.mymq.broker;


import bao.study.mymq.broker.longpolling.PullRequestHoldService;
import bao.study.mymq.broker.manager.CommitLogManager;
import bao.study.mymq.broker.manager.ConsumeQueueOffsetManager;
import bao.study.mymq.broker.manager.ConsumeQueueManager;
import bao.study.mymq.broker.processor.PullMessageProcessor;
import bao.study.mymq.broker.store.CommitLog;

/**
 * @author baoyh
 * @since 2022/10/14 10:28
 */
public class BrokerController {

    private final ConsumeQueueOffsetManager consumeQueueOffsetManager;

    private final ConsumeQueueManager consumeQueueManager;

    private final CommitLogManager commitLogManager;

    private final PullMessageProcessor pullMessageProcessor;

    private final PullRequestHoldService pullRequestHoldService;

    public BrokerController(ConsumeQueueOffsetManager consumeQueueOffsetManager, ConsumeQueueManager consumeQueueManager, CommitLogManager commitLogManager) {
        this.consumeQueueOffsetManager = consumeQueueOffsetManager;
        this.consumeQueueManager = consumeQueueManager;
        this.commitLogManager = commitLogManager;

        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
    }

    protected boolean initialize() {
        boolean result = consumeQueueOffsetManager.load();
        result = result && consumeQueueManager.load();
        result = result && commitLogManager.load();
        return result;
    }

    protected void start() {
        pullRequestHoldService.start();
    }

    public ConsumeQueueOffsetManager getConsumeOffsetManager() {
        return consumeQueueOffsetManager;
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
