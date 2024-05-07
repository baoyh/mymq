package bao.study.mymq.common.protocol.raft;

/**
 * @author baoyh
 * @since 2024/4/7 17:50
 */
public class BaseProtocol {

    protected String remoteId;

    protected String localId;

    protected int code;

    protected String leaderId;

    protected long term;

    public String getRemoteId() {
        return remoteId;
    }

    public void setRemoteId(String remoteId) {
        this.remoteId = remoteId;
    }

    public String getLocalId() {
        return localId;
    }

    public void setLocalId(String localId) {
        this.localId = localId;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String baseInfo() {
        return String.format("info[term=%d,code=%d,local=%s,remote=%s,leader=%s]", term, code, localId, remoteId, leaderId);
    }
}
