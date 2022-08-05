package bao.study.mymq.remoting;

/**
 * @author baoyh
 * @since 2022/8/5 13:27
 */
public class RemotingException extends RuntimeException {

    private String message;

    public RemotingException(String message) {
        super(message);
    }

    public RemotingException(String message, Throwable cause) {
        super(message, cause);
    }
}
