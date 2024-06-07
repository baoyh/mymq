package bao.study.mymq.broker.raft.store;

import bao.study.mymq.broker.store.MappedFile;
import bao.study.mymq.common.protocol.raft.RaftEntry;

import java.util.List;

/**
 * @author baoyh
 * @since 2024/5/21 16:25
 */
public abstract class RaftStore {

    public abstract RaftEntry append(RaftEntry entry);

    /**
     * 根据日志下标查找日志
     */
    public abstract RaftEntry get(long index);

    public abstract long getCommittedIndex();

    /**
     * 更新 committedIndex 的值
     */
    public abstract void updateCommittedIndex(long term, long committedIndex);

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

    public abstract List<MappedFile> getDataFileList();


    public void flush() {

    }

    /**
     * 删除日志
     */
    public long truncate(RaftEntry entry, long leaderTerm, String leaderId) {
        return -1;
    }

    public abstract void startup();

    public abstract void shutdown();
}
