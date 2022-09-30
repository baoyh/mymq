package bao.study.mymq.broker;

/**
 * @author baoyh
 * @since 2022/8/5 13:27
 */
public class BrokerException extends RuntimeException {

    private String message;

    public BrokerException(String message) {
        super(message);
    }

    public BrokerException(String message, Throwable cause) {
        super(message, cause);
    }

    public BrokerException(Throwable cause) {
        super(cause);
    }

}
