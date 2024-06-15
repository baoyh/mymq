package bao.study.mymq.router.processor;

import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.netty.NettyRequestProcessor;
import bao.study.mymq.router.routeinfo.RouterInfoManager;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author baoyh
 * @since 2022/5/16 14:47
 */
public class RouterRequestProcessor implements NettyRequestProcessor {

    RouterInfoManager routerInfoManager = new RouterInfoManager();

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        int code = msg.getCode();

        switch (code) {
            case RequestCode.REGISTER_BROKER:
                return registerBroker(msg);
            case RequestCode.GET_ROUTE_BY_TOPIC:
                return getRouteByTopic(msg);
            case RequestCode.BROKER_HEARTBEAT:
                return handleHeartbeat(msg);
            case RequestCode.QUERY_ALIVE_BROKERS:
                return queryAliveBrokers(msg);
            default:
                return null;
        }
    }

    private RemotingCommand registerBroker(RemotingCommand msg) {
        return routerInfoManager.registerBroker(msg);
    }

    private RemotingCommand getRouteByTopic(RemotingCommand msg) {
        return routerInfoManager.getRouteByTopic(msg);
    }

    private RemotingCommand handleHeartbeat(RemotingCommand msg) {
        return routerInfoManager.handleHeartbeat(msg);
    }

    private RemotingCommand queryAliveBrokers(RemotingCommand msg) {
        return routerInfoManager.queryAliveBrokers(msg);
    }

    public void start() {
        routerInfoManager.start();
    }

    public void shutdown() {
        routerInfoManager.shutdown();
    }
}
