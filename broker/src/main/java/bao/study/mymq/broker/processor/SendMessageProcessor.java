package bao.study.mymq.broker.processor;

import bao.study.mymq.common.protocol.Message;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.code.ResponseCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandType;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * @author baoyh
 * @since 2022/8/18 13:53
 */
public class SendMessageProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(SendMessageProcessor.class);

    @Override
    public RemotingCommand processRequest(RemotingCommand msg) {
        log.info("do store message " + new String(CommonCodec.decode(msg.getBody(), Message.class).getBody(), StandardCharsets.UTF_8));
        msg.setRemotingCommandType(RemotingCommandType.RESPONSE);
        msg.setCode(ResponseCode.SUCCESS);
        return msg;
    }
}
