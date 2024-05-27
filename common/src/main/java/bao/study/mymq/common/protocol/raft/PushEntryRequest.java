package bao.study.mymq.common.protocol.raft;

/**
 * @author baoyh
 * @since 2024/5/23 16:49
 */
public class PushEntryRequest extends BaseProtocol {

    private long commitIndex = -1;

    private Type type = Type.APPEND;

    private RaftEntry entry;

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public RaftEntry getEntry() {
        return entry;
    }

    public void setEntry(RaftEntry entry) {
        this.entry = entry;
    }

    public enum Type {
        APPEND,
        COMMIT,
        COMPARE,
        TRUNCATE
    }
}
