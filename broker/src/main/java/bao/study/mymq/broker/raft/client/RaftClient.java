package bao.study.mymq.broker.raft.client;

import bao.study.mymq.common.protocol.raft.AppendEntryRequest;
import bao.study.mymq.common.protocol.raft.AppendEntryResponse;


/**
 * @author baoyh
 */
public class RaftClient {

    private final String leaderAddress;

    private final RaftClientRpcService raftClientRpcService;

    public RaftClient(String leaderAddress) {
        this.leaderAddress = leaderAddress;
        this.raftClientRpcService = new RaftClientRpcService();
    }

    public AppendEntryResponse append(byte[] message) {
        AppendEntryRequest entryRequest = new AppendEntryRequest();
        entryRequest.setBody(message);
        return raftClientRpcService.append(entryRequest, leaderAddress);
    }

    public void startup() {
        raftClientRpcService.startup();
    }
}
