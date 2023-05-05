package bao.study.mymq.broker.processor;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.body.PullMessageBody;
import bao.study.mymq.common.protocol.body.QueryConsumerOffsetBody;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ConcurrentMap;

import static bao.study.mymq.remoting.code.RequestCode.*;
import static bao.study.mymq.remoting.code.ResponseCode.*;

/**
 * @author baoyh
 * @since 2022/10/14 10:12
 */
public class ConsumerManageProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public ConsumerManageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        switch (msg.getCode()) {
            case QUERY_CONSUMER_OFFSET:
                return queryBrokerOffset(msg);
            case PULL_MESSAGE:
                return pullMessage(msg);
            default:
                return null;
        }
    }

    private RemotingCommand queryBrokerOffset(RemotingCommand msg) {
        QueryConsumerOffsetBody body = CommonCodec.decode(msg.getBody(), QueryConsumerOffsetBody.class);
        Long offset = brokerController.getConsumerOffsetManager().getConsumedOffset().get(body.getTopic() + Constant.TOPIC_SEPARATOR + body.getGroup()).get(body.getQueueId());
        return RemotingCommandFactory.createResponseRemotingCommand(SUCCESS, String.valueOf(offset).getBytes());
    }

    private RemotingCommand pullMessage(RemotingCommand msg) {
        PullMessageBody body = CommonCodec.decode(msg.getBody(), PullMessageBody.class);
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> consumedOffset = brokerController.getConsumerOffsetManager().getConsumedOffset();
        ConcurrentMap<Integer, Long> offsetTable = consumedOffset.get(body.getTopic() + Constant.TOPIC_SEPARATOR + body.getGroup());
        if (offsetTable != null) {
            Long offset = offsetTable.get(body.getQueueId());
        }
        return null;
    }
}
