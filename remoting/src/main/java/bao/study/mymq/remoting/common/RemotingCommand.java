package bao.study.mymq.remoting.common;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author baoyh
 * @since 2022/5/13 15:05
 */
public class RemotingCommand {

    private static final AtomicInteger commandId = new AtomicInteger(0);

    private int code;

    private int requestId = commandId.getAndIncrement();

    private byte[] body;

    private RemotingCommandType remotingCommandType;

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

    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public RemotingCommandType getRemotingCommandType() {
        return remotingCommandType;
    }

    public void setRemotingCommandType(RemotingCommandType remotingCommandType) {
        this.remotingCommandType = remotingCommandType;
    }

    @Override
    public String toString() {
        String s = body == null ? "null" : new String(body, StandardCharsets.UTF_8);
        return "RemotingCommand{" +
                "code=" + code +
                ", requestId=" + requestId +
                ", body=" + s +
                ", remotingCommandType=" + remotingCommandType +
                '}';
    }
}
