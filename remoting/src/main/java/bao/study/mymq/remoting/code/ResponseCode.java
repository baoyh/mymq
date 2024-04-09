package bao.study.mymq.remoting.code;

/**
 * @author baoyh
 * @since 2022/8/1 16:57
 */
public abstract class ResponseCode {

    public static final int SUCCESS = 0;

    public static final int NETWORK_ERROR = 1;

    /**
     * consume
     */
    public static final int FOUND_MESSAGE = 10;

    public static final int NOT_FOUND_MESSAGE = 11;


    /**
     * raft
     */
    public static final int EXPIRED_TERM = 200;

    public static final int TERM_NOT_READY = 201;

    public static final int INCONSISTENT_LEADER = 202;


}
