package bao.study.mymq.broker.raft.client;

import bao.study.mymq.broker.raft.Config;
import bao.study.mymq.common.protocol.raft.AppendEntryRequest;
import bao.study.mymq.common.protocol.raft.AppendEntryResponse;
import bao.study.mymq.common.utils.CommonCodec;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.common.RemotingCommandFactory;
import bao.study.mymq.remoting.netty.NettyClient;

/**
 * @author baoyh
 * @since 2024/5/21 15:07
 */
public class RaftClientRpcService {

    private final RemotingClient client;

    private final Config config;

    public RaftClientRpcService() {
        this.client = new NettyClient();
        this.config = new Config();
    }

    public AppendEntryResponse append(AppendEntryRequest entryRequest, String leaderAddress) {
        RemotingCommand request = RemotingCommandFactory.createRequestRemotingCommand(RequestCode.APPEND, CommonCodec.encode(entryRequest));
        RemotingCommand response = client.invokeSync(leaderAddress, request, config.getRpcTimeoutMillis());
        return CommonCodec.decode(response.getBody(), AppendEntryResponse.class);
    }

    public void startup() {
        client.start();
    }
}
