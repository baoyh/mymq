package bao.study.mymq.broker.longpolling;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.util.MessageStoreHelper;
import bao.study.mymq.common.ServiceThread;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author baoyh
 * @since 2023/6/28 16:34
 */
public class PullRequestHoldService extends ServiceThread {

    private final static Logger log = LoggerFactory.getLogger(PullRequestHoldService.class);

    private final BrokerController brokerController;

    private final ConcurrentHashMap<String /*topic@queueId*/, CopyOnWriteArrayList<PullRequest>> pullRequestTable = new ConcurrentHashMap<>();

    public PullRequestHoldService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void run() {

        while (!stop) {
            waitForRunning(5 * 1000);
            try {
                pullRequestTable.forEach((k, v) -> wakeupAll(v, true));
            } catch (Exception e) {
                log.error("Hold pull request fail ", e);
            }

        }
    }

    public void add(String key, PullRequest pullRequest) {
        CopyOnWriteArrayList<PullRequest> pullRequests = pullRequestTable.get(key);

        if (pullRequests == null) {
            pullRequests = new CopyOnWriteArrayList<>();
            pullRequests.add(pullRequest);
            pullRequestTable.put(key, pullRequests);
        } else {
            pullRequests.add(pullRequest);
        }
    }

    private void wakeupAll(CopyOnWriteArrayList<PullRequest> pullRequests, boolean checkTimeout) {
        if (pullRequests != null && !pullRequests.isEmpty()) {
            for (PullRequest pullRequest : pullRequests) {
                if (checkTimeout) {
                    if (System.currentTimeMillis() >= (pullRequest.getBeginTime() + pullRequest.getHoldTime())) {
                        wakeup(pullRequest);
                        pullRequests.remove(pullRequest);
                    }
                } else {
                    wakeup(pullRequest);
                    pullRequests.remove(pullRequest);
                }
            }
        }
    }

    public void wakeupWhenMessageArriving(String topic, int queueId) {
        CopyOnWriteArrayList<PullRequest> pullRequests = pullRequestTable.get(MessageStoreHelper.createKey(topic, queueId));
        wakeupAll(pullRequests, false);
    }

    private void wakeup(PullRequest pullRequest) {
        RemotingCommand response = brokerController.getPullMessageProcessor().pullMessage(pullRequest.getRemotingCommand(), pullRequest.getChannel(), false);
        if (response != null) {
            response.setRequestId(pullRequest.getRequestId());
            response.setRemotingCommandType(RemotingCommandType.RESPONSE);
            pullRequest.getChannel().writeAndFlush(response);
        }
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

}
