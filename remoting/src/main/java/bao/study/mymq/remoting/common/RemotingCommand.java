package bao.study.mymq.remoting.common;

/**
 * @author baoyh
 * @since 2022/5/13 15:05
 */
public class RemotingCommand {

    private int code;

    private byte[] body;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
