package bao.study.mymq.broker;

import bao.study.mymq.broker.raft.Config;
import bao.study.mymq.broker.raft.MemberState;
import bao.study.mymq.broker.raft.RaftServer;
import bao.study.mymq.broker.raft.Role;
import bao.study.mymq.remoting.RemotingUtil;
import bao.study.mymq.remoting.netty.NettyClient;
import bao.study.mymq.remoting.netty.NettyServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author baoyh
 * @since 2024/4/9 13:38
 */
public class RaftTest {

    @Test
    public void testThreeServer() throws InterruptedException {
        RaftServer a = createRaftServer(11000);
        RaftServer b = createRaftServer(11001);
        RaftServer c = createRaftServer(11002);

        // 真实环境中会通过注册中心获取节点地址
        Map<String, String> nodes = new HashMap<>();
        registerNodes(nodes, a, 11000);
        registerNodes(nodes, b, 11001);
        registerNodes(nodes, c, 11002);
//        MemberState memberState = a.getMemberState();
//        memberState.setRole(Role.LEADER);
//        memberState.setLeaderId(memberState.getSelfId());

        updateNodes(nodes, a, b, c);
        startServer(a, b, c);

        Thread.sleep(6000);

        AtomicInteger leaderNum = new AtomicInteger();
        AtomicInteger followerNum = new AtomicInteger();
        countNum(leaderNum, followerNum, a, b, c);

        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());
    }

    private void countNum(AtomicInteger leaderNum, AtomicInteger followerNum, RaftServer... servers) {
        for (RaftServer server : servers) {
            if (server.getMemberState().getRole() == Role.FOLLOWER) {
                followerNum.incrementAndGet();
            } else if (server.getMemberState().getRole() == Role.LEADER) {
                leaderNum.incrementAndGet();
            }
        }
    }

    private void startServer(RaftServer... servers) {
        for (RaftServer server : servers) {
            server.startup();
        }
    }

    private RaftServer createRaftServer(int port) {
        NettyServer nettyServer = new NettyServer(port);
        nettyServer.start();

        NettyClient nettyClient = new NettyClient();
        nettyClient.start();

        return new RaftServer(new Config(), nettyClient, nettyServer);
    }

    private void registerNodes(Map<String, String> nodes, RaftServer server, int port) {
        MemberState memberState = server.getMemberState();
        nodes.putIfAbsent(memberState.getSelfId(), RemotingUtil.getLocalAddress() + ":" + port);
    }

    private void updateNodes(Map<String, String> nodes, RaftServer... servers) {

        Map<String, Boolean> liveNodes = new HashMap<>();
        nodes.keySet().forEach(it -> liveNodes.put(it, true));

        for (RaftServer server : servers) {
            server.getMemberState().setNodes(nodes);
            server.getMemberState().setLiveNodes(liveNodes);
        }
    }
}
