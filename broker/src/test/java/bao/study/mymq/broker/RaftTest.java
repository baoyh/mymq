package bao.study.mymq.broker;

import bao.study.mymq.broker.raft.Config;
import bao.study.mymq.broker.raft.MemberState;
import bao.study.mymq.broker.raft.RaftServer;
import bao.study.mymq.broker.raft.Role;
import bao.study.mymq.broker.raft.client.RaftClient;
import bao.study.mymq.common.protocol.raft.AppendEntryResponse;
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

        // 等待选举完成
        // 极低概率出现轮次过多导致选举时间过长,可以适当调整间隔
        Thread.sleep(2000);

        AtomicInteger leaderNum = new AtomicInteger();
        AtomicInteger followerNum = new AtomicInteger();

        RaftServer leader = countNum(leaderNum, followerNum, a, b, c);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());

        // 模拟 leader 宕机, 重新发起选举
        leader.shutdown();
        Thread.sleep(2000);

        countNum(leaderNum, followerNum, a, b, c);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(1, followerNum.get());

        // 宕机的 leader 重新启动, 变成新的 follower
        leader.startup();
        Thread.sleep(2000);

        countNum(leaderNum, followerNum, a, b, c);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());
    }

    @Test
    public void testEntryAppend() throws InterruptedException {
        RaftServer a = createRaftServer(11000);
        RaftServer b = createRaftServer(11001);
        // 真实环境中会通过注册中心获取节点地址
        Map<String, String> nodes = new HashMap<>();
        registerNodes(nodes, a, 11000, "node-a");
        registerNodes(nodes, b, 11001, "node-b");
        updateNodes(nodes, a, b);
        startServer(a, b);

        Thread.sleep(2000);

        String leaderAddress = getLeaderAddress(a, b);
        Assertions.assertNotEquals(null, leaderAddress);
        RaftClient raftClient = new RaftClient(leaderAddress);
        raftClient.startup();

        for (int i = 0; i < 10; i++) {
            AppendEntryResponse entryResponse = raftClient.append(("test first entry" + i).getBytes());
            System.out.println(entryResponse);
            Assertions.assertEquals(i, entryResponse.getIndex());
        }
    }

    private String getLeaderAddress(RaftServer... servers) {
        for (RaftServer server : servers) {
            if (!server.getAlive()) continue;
            if (server.getMemberState().getRole() == Role.LEADER) {
                return server.getMemberState().getNodes().get(server.getMemberState().getSelfId());
            }
        }
        return null;
    }


    private RaftServer countNum(AtomicInteger leaderNum, AtomicInteger followerNum, RaftServer... servers) {
        leaderNum.set(0);
        followerNum.set(0);
        RaftServer leader = null;
        for (RaftServer server : servers) {
            if (!server.getAlive()) continue;
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
        return new RaftServer(new Config(), new NettyClient(), new NettyServer(port));
    }

    private void registerNodes(Map<String, String> nodes, RaftServer server, int port) {
        MemberState memberState = server.getMemberState();
        nodes.putIfAbsent(memberState.getSelfId(), RemotingUtil.getLocalAddress() + ":" + port);
    }

    private void registerNodes(Map<String, String> nodes, RaftServer server, int port, String id) {
        MemberState memberState = server.getMemberState();
        memberState.setSelfId(id);
        memberState.getConfig().setSelfId(id);
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
