package bao.study.mymq.broker.processor;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.common.protocol.broker.FlushMessage;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author baoyh
 * @since 2024/6/12 15:53
 */
public class FlushSyncProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public FlushSyncProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        FlushMessage flushMessage = CommonCodec.decode(msg.getBody(), FlushMessage.class);
        brokerController.getCommitLogManager().updateCommittedTable();
        brokerController.getConsumeQueueManager().updateWhenMessageArriving(flushMessage.getTopic(), flushMessage.getQueueId(), flushMessage.getOffset(), flushMessage.getSize());
        brokerController.getPullRequestHoldService().wakeupWhenMessageArriving(flushMessage.getTopic(), flushMessage.getQueueId());
        return RemotingCommandFactory.createResponseRemotingCommand(ResponseCode.SUCCESS, null);
    }
}
