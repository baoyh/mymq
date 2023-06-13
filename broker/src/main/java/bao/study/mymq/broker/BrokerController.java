package bao.study.mymq.broker;


import bao.study.mymq.broker.manager.CommitLogManager;
import bao.study.mymq.broker.manager.ConsumeOffsetManager;
import bao.study.mymq.broker.manager.ConsumeQueueManager;
import bao.study.mymq.broker.store.CommitLog;

/**
 * @author baoyh
 * @since 2022/10/14 10:28
 */
public class BrokerController {

    private final ConsumeOffsetManager consumeOffsetManager;

    private final ConsumeQueueManager consumeQueueManager;

    private final CommitLogManager commitLogManager;

    public BrokerController(ConsumeOffsetManager consumeOffsetManager, ConsumeQueueManager consumeQueueManager, CommitLogManager commitLogManager) {
        this.consumeOffsetManager = consumeOffsetManager;
        this.consumeQueueManager = consumeQueueManager;
        this.commitLogManager = commitLogManager;
    }

    public boolean initialize() {
        boolean result = consumeOffsetManager.load();
        result = result && consumeQueueManager.load();
        result = result && commitLogManager.load();
        return result;
    }

    public ConsumeOffsetManager getConsumeOffsetManager() {
        return consumeOffsetManager;
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
}
