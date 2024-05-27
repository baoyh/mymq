package bao.study.mymq.common.protocol.raft;

/**
 * @author baoyh
 * @since 2024/5/21 14:45
 */
public class AppendEntryRequest {

    private byte[] body;

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
