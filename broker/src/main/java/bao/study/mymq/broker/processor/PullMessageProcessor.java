package bao.study.mymq.broker.processor;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.broker.longpolling.PullRequest;
import bao.study.mymq.broker.manager.ConsumeQueueIndexManager;
import bao.study.mymq.broker.store.CommitLog;
import bao.study.mymq.broker.store.ConsumeQueue;
import bao.study.mymq.broker.store.ConsumeQueueOffset;
import bao.study.mymq.broker.store.MessageStore;
import bao.study.mymq.broker.util.MessageStoreHelper;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.MessageExt;
import bao.study.mymq.common.protocol.body.PullMessageBody;
import bao.study.mymq.common.protocol.body.QueryConsumerOffsetBody;
import bao.study.mymq.common.protocol.body.SendMessageBackBody;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(PullMessageProcessor.class);

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
            case CONSUMER_SEND_MSG_BACK:
                return sendMessageBack(msg);
            default:
                return null;
        }
    }

    private RemotingCommand queryBrokerOffset(RemotingCommand msg) {
        QueryConsumerOffsetBody body = CommonCodec.decode(msg.getBody(), QueryConsumerOffsetBody.class);
        Long offset = brokerController.getConsumeOffsetManager().getConsumedOffset().get(body.getTopic() + Constant.TOPIC_SEPARATOR + body.getGroup()).get(body.getQueueId());
        return RemotingCommandFactory.createResponseRemotingCommand(SUCCESS, String.valueOf(offset).getBytes());
    }

    public RemotingCommand pullMessage(RemotingCommand msg, Channel channel, boolean hold) {

        PullMessageBody body = CommonCodec.decode(msg.getBody(), PullMessageBody.class);
        ConsumeQueueIndexManager consumeQueueIndexManager = brokerController.getConsumeOffsetManager();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> consumedOffset = consumeQueueIndexManager.getConsumedOffset();
        consumeQueueIndexManager.checkConsumedOffset(body.getTopic(), body.getGroup(), body.getQueueId());
        ConcurrentMap<Integer, Long> offsetTable = consumedOffset.get(body.getTopic() + Constant.TOPIC_SEPARATOR + body.getGroup());

        String key = MessageStoreHelper.createKey(body.getTopic(), body.getQueueId());
        ConcurrentHashMap<String, ConsumeQueue> consumeQueueTable = brokerController.getConsumeQueueManager().getConsumeQueueTable();
        ConsumeQueue consumeQueue = consumeQueueTable.get(key);
        List<MessageExt> messages = new ArrayList<>();
        if (offsetTable != null && consumeQueue != null) {
            Long consumedQueueIndexOffset = offsetTable.get(body.getQueueId());
            List<ConsumeQueueOffset> consumeQueueOffsets = consumeQueue.pullMessage(consumedQueueIndexOffset);
            if (!consumeQueueOffsets.isEmpty()) {
                CommitLog commitLog = brokerController.getCommitLog();
                for (int i = 0; i < consumeQueueOffsets.size(); i++) {
                    ConsumeQueueOffset offset = consumeQueueOffsets.get(i);
                    MessageStore read = commitLog.read(offset.getOffset(), offset.getSize());
                    MessageExt messageExt = messageStore2MessageExt(read);
                    messageExt.setGroup(body.getGroup());
                    messageExt.setOffset(consumedQueueIndexOffset + i + 1);
                    messages.add(messageExt);
                }
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


    private RemotingCommand sendMessageBack(RemotingCommand msg) {
        SendMessageBackBody body = CommonCodec.decode(msg.getBody(), SendMessageBackBody.class);
        if (body.isStatus()) {
            // consume success
            brokerController.getConsumeOffsetManager().updateConsumedOffset(body.getTopic(), body.getGroup(), body.getQueueId(),
                    body.getOffset());
        }
        return null;
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
