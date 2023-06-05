package bao.study.mymq.broker;


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

    private final CommitLog commitLog;

    public BrokerController(ConsumeOffsetManager consumeOffsetManager, ConsumeQueueManager consumeQueueManager, CommitLog commitLog) {
        this.consumeOffsetManager = consumeOffsetManager;
        this.consumeQueueManager = consumeQueueManager;
        this.commitLog = commitLog;
    }

    public boolean initialize() {
        boolean result = consumeOffsetManager.load();
        result = result && consumeQueueManager.load();
        return result;
    }

    public ConsumeOffsetManager getConsumeOffsetManager() {
        return consumeOffsetManager;
    }

    public ConsumeQueueManager getConsumeQueueManager() {
        return consumeQueueManager;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }
}
