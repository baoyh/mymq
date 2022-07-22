package bao.study.mymq.remoting.common;

import java.io.Serializable;

/**
 * @author baoyh
 * @since 2022/5/13 15:05
 */
public class RemotingCommand implements Serializable {

    private static final long serialVersionUID = -5734509523963527363L;

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
