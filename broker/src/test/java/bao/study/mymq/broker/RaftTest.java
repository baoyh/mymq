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
        registerNodes(nodes, a, 11000, "node-a");
        registerNodes(nodes, b, 11001, "node-b");
        registerNodes(nodes, c, 11002, "node-c");

        updateNodes(nodes, a, b, c);
        startServer(a, b, c);

        System.out.println(a.getRemotingClient());
        System.out.println(b.getRemotingClient());
        System.out.println(c.getRemotingClient());

        Thread.sleep(3000);

        AtomicInteger leaderNum = new AtomicInteger();
        AtomicInteger followerNum = new AtomicInteger();
        RaftServer leader = countNum(leaderNum, followerNum, a, b, c);

        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());

        leader.shutdown();
        Thread.sleep(2000);

        leaderNum.set(0);
        followerNum.set(0);
        countNum(leaderNum, followerNum, a, b, c);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(1, followerNum.get());
    }

    private RaftServer countNum(AtomicInteger leaderNum, AtomicInteger followerNum, RaftServer... servers) {
        RaftServer leader = null;
        for (RaftServer server : servers) {
            if (!server.isAlive()) continue;
            if (server.getMemberState().getRole() == Role.FOLLOWER) {
                followerNum.incrementAndGet();
            } else if (server.getMemberState().getRole() == Role.LEADER) {
                leaderNum.incrementAndGet();
                leader = server;
            }
        }
        return leader;
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

    private void registerNodes(Map<String, String> nodes, RaftServer server, int port, String id) {
        MemberState memberState = server.getMemberState();
        memberState.setSelfId(id);
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
