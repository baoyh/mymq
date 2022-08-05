package bao.study.mymq.client;

/**
 * @author baoyh
 * @since 2022/8/3 11:27
 */
public class ClientException extends RuntimeException {

    private String message;

    public ClientException(String message) {
        super(message);
        this.message = message;
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
