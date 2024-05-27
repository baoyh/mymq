package bao.study.mymq.broker.raft.store;

import bao.study.mymq.common.protocol.raft.RaftEntry;

/**
 * @author baoyh
 * @since 2024/5/21 16:25
 */
public abstract class RaftStore {

    public abstract RaftEntry appendAsLeader(RaftEntry entry);

    /**
     * 向从节点同步日志
     */
    public abstract RaftEntry appendAsFollower(RaftEntry entry, long leaderTerm, String leaderId);

    /**
     * 根据日志下标查找日志
     */
    public abstract RaftEntry get(long index);

    public abstract long getCommittedIndex();

    /**
     * 更新 committedIndex 的值
     */
    public void updateCommittedIndex(long term, long committedIndex) {

    }

    /**
     * 获取 Leader 当前最大的投票轮次
     */
    public abstract long getEndTerm();

    /**
     * 获取 Leader 下一条日志写入的下标
     */
    public abstract long getEndIndex();

    /**
     * 获取 Leader 第一条消息的下标
     */
    public abstract long getBeginIndex();


    public void flush() {

    }

    /**
     * 删除日志
     */
    public long truncate(RaftEntry entry, long leaderTerm, String leaderId) {
        return -1;
    }

    public void startup() {

    }

    public void shutdown() {

    }
}
