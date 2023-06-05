package bao.study.mymq.broker.processor;

import bao.study.mymq.broker.BrokerController;
import bao.study.mymq.broker.store.ConsumeQueueOffset;
import bao.study.mymq.broker.store.MessageStore;
import bao.study.mymq.common.protocol.MessageExt;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandType;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;


/**
 * @author baoyh
 * @since 2022/8/18 13:53
 */
public class SendMessageProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(SendMessageProcessor.class);

    private final BrokerController brokerController;

    public SendMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        MessageExt messageExt = CommonCodec.decode(msg.getBody(), MessageExt.class);
        MessageStore messageStore = createMessageStore(messageExt, ctx);

        ConsumeQueueOffset offset = brokerController.getCommitLog().appendMessage(messageStore);
//        brokerController.getConsumeQueueManager().updateConsumeQueueTable(messageExt.getTopic());

        msg.setRemotingCommandType(RemotingCommandType.RESPONSE);
        msg.setCode(ResponseCode.SUCCESS);
        return msg;
    }

    private MessageStore createMessageStore(MessageExt messageExt, ChannelHandlerContext ctx) {
        MessageStore messageStore = new MessageStore();
        messageStore.setBornHost((InetSocketAddress) ctx.channel().remoteAddress());
        messageStore.setStoreTimeStamp(System.currentTimeMillis());
        messageStore.setBornTimeStamp(messageExt.getBornTimeStamp());
        messageStore.setBrokerName(messageExt.getBrokerName());
        messageStore.setTopic(messageExt.getTopic());
        messageStore.setBody(messageExt.getBody());
        messageStore.setQueueId(0);
        return messageStore;
    }
}
