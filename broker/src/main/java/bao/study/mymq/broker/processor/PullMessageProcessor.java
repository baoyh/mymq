package bao.study.mymq.broker.processor;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.broker.longpolling.PullRequest;
import bao.study.mymq.broker.manager.ConsumeQueueOffsetManager;
import bao.study.mymq.broker.store.CommitLog;
import bao.study.mymq.broker.store.ConsumeQueue;
import bao.study.mymq.broker.store.ConsumeQueueOffset;
import bao.study.mymq.broker.store.MessageStore;
import bao.study.mymq.broker.util.MessageStoreHelper;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.MessageExt;
import bao.study.mymq.common.protocol.body.PullMessageBody;
import bao.study.mymq.common.protocol.body.QueryConsumerOffsetBody;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static bao.study.mymq.remoting.code.RequestCode.*;
import static bao.study.mymq.remoting.code.ResponseCode.*;

/**
 * @author baoyh
 * @since 2022/10/14 10:12
 */
public class PullMessageProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public PullMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        switch (msg.getCode()) {
            case QUERY_CONSUMER_OFFSET:
                return queryBrokerOffset(msg);
            case PULL_MESSAGE:
                return pullMessage(msg, ctx.channel(), true);
            default:
                return null;
        }
    }

    @Override
    public void callback(ChannelFuture listener) {
        if (listener.isSuccess()) {

        }
    }

    private RemotingCommand queryBrokerOffset(RemotingCommand msg) {
        QueryConsumerOffsetBody body = CommonCodec.decode(msg.getBody(), QueryConsumerOffsetBody.class);
        Long offset = brokerController.getConsumeOffsetManager().getConsumedOffset().get(body.getTopic() + Constant.TOPIC_SEPARATOR + body.getGroup()).get(body.getQueueId());
        return RemotingCommandFactory.createResponseRemotingCommand(SUCCESS, String.valueOf(offset).getBytes());
    }

    public RemotingCommand pullMessage(RemotingCommand msg, Channel channel, boolean hold) {

        PullMessageBody body = CommonCodec.decode(msg.getBody(), PullMessageBody.class);
        ConsumeQueueOffsetManager consumeQueueOffsetManager = brokerController.getConsumeOffsetManager();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> consumedOffset = consumeQueueOffsetManager.getConsumedOffset();
        ConcurrentMap<Integer, Long> offsetTable = consumedOffset.get(body.getTopic() + Constant.TOPIC_SEPARATOR + body.getGroup());

        String key = MessageStoreHelper.createKey(body.getTopic(), body.getQueueId());
        ConcurrentHashMap<String, ConsumeQueue> consumeQueueTable = brokerController.getConsumeQueueManager().getConsumeQueueTable();
        ConsumeQueue consumeQueue = consumeQueueTable.get(key);
        List<MessageExt> messages = new ArrayList<>();
        if (offsetTable != null && consumeQueue != null) {
            List<ConsumeQueueOffset> consumeQueueOffsets = consumeQueue.pullMessage(offsetTable.get(body.getQueueId()));
            if (!consumeQueueOffsets.isEmpty()) {
                CommitLog commitLog = brokerController.getCommitLog();
                for (ConsumeQueueOffset offset : consumeQueueOffsets) {
                    MessageStore read = commitLog.read(offset.getOffset(), offset.getSize());
                    messages.add(messageStore2MessageExt(read));
                }

                consumeQueueOffsetManager.updateConsumedOffset(body.getTopic(), body.getGroup(), body.getQueueId(),
                        (long) consumeQueueOffsets.size());
            }
        }

        if (messages.isEmpty()) {
            if (hold) {
                PullRequest pullRequest = new PullRequest(msg, channel, msg.getRequestId(), BrokerConfig.getHoldTime(), System.currentTimeMillis());
                brokerController.getPullRequestHoldService().add(key, pullRequest);
                return null;
            } else {
                return RemotingCommandFactory.createResponseRemotingCommand(NOT_FOUND_MESSAGE, null);
            }

        } else {
            return RemotingCommandFactory.createResponseRemotingCommand(FOUND_MESSAGE, CommonCodec.encode(messages));
        }
    }

    private MessageExt messageStore2MessageExt(MessageStore store) {
        MessageExt messageExt = new MessageExt();
        messageExt.setBornTimeStamp(store.getBornTimeStamp());
        messageExt.setTopic(store.getTopic());
        messageExt.setBody(store.getBody());
        messageExt.setBrokerName(store.getBrokerName());
        messageExt.setQueueId(store.getQueueId());
        return messageExt;
    }
}
