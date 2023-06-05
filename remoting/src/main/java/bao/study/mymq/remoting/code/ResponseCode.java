package bao.study.mymq.remoting.code;

/**
 * @author baoyh
 * @since 2022/8/1 16:57
 */
public abstract class ResponseCode {

    public static final int SUCCESS = 0;

    /**
     * consume
     */
    public static final int FOUND_MESSAGE = 10;

    public static final int NOT_FOUND_MESSAGE = 11;
}
