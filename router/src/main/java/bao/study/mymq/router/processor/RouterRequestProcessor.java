package bao.study.mymq.router.processor;

import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;

import java.util.Arrays;

/**
 * @author baoyh
 * @since 2022/5/16 14:47
 */
public class RouterRequestProcessor implements NettyRequestProcessor {



    @Override
    public void processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        int code = msg.getCode();

        switch (code) {
            case RequestCode.REGISTER_STORE:
                registerStore(ctx, msg);
                break;
            case RequestCode.GET_ROUTE_BY_TOPIC:
                getRouteByTopic(ctx, msg);
                break;
            default:
                break;
        }
    }

    private void registerStore(ChannelHandlerContext ctx, RemotingCommand msg) {
        System.out.println(Arrays.toString(msg.getBody()));
    }

    private void getRouteByTopic(ChannelHandlerContext ctx, RemotingCommand msg) {

    }

}
