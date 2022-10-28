package bao.study.mymq.broker.processor;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.common.Constant;
import bao.study.mymq.common.protocol.body.QueryConsumerOffsetBody;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;

import static bao.study.mymq.remoting.code.RequestCode.QUERY_CONSUMER_OFFSET;
import static bao.study.mymq.remoting.code.ResponseCode.SUCCESS;

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
            default:
                return null;
        }
    }

    private RemotingCommand queryBrokerOffset(RemotingCommand msg) {
        QueryConsumerOffsetBody body = CommonCodec.decode(msg.getBody(), QueryConsumerOffsetBody.class);
        Long offset = brokerController.getConsumerOffsetManager().getOffsetTable().get(body.getTopic() + Constant.TOPIC_GROUP_SEPARATOR + body.getGroup()).get(body.getQueueId());
        return RemotingCommandFactory.createResponseRemotingCommand(SUCCESS, String.valueOf(offset).getBytes());
    }
}
