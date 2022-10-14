package bao.study.mymq.broker.processor;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.common.protocol.body.QueryConsumerOffsetBody;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ConcurrentMap;

import static bao.study.mymq.remoting.code.RequestCode.QUERY_CONSUMER_OFFSET;
import static bao.study.mymq.remoting.code.ResponseCode.SUCCESS;

/**
 * @author baoyh
 * @since 2022/10/14 10:12
 */
public class ConsumerManageProcessor implements NettyRequestProcessor {

    private static final String TOPIC_GROUP_SEPARATOR = "@";

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        switch (msg.getCode()) {
            case QUERY_CONSUMER_OFFSET:
                return queryBrokerOffset(msg);
            default:
                return null;
        }
    }

    private RemotingCommand queryBrokerOffset(RemotingCommand msg) {
        QueryConsumerOffsetBody body = CommonCodec.decode(msg.getBody(), QueryConsumerOffsetBody.class);
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = BrokerController.getOffsetTable();

        Long offset = offsetTable.get(body.getTopic() + TOPIC_GROUP_SEPARATOR + body.getGroup()).get(body.getQueueId());

        return RemotingCommandFactory.createResponseRemotingCommand(SUCCESS, String.valueOf(offset).getBytes());
    }
}
