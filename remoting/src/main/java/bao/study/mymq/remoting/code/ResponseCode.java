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
     * raft heartbeat
     */
    public static final int EXPIRED_TERM = 200;

    public static final int TERM_NOT_READY = 201;

    public static final int INCONSISTENT_LEADER = 202;

    /**
     * raft vote
     */
    public static final int REJECT_ALREADY_VOTED = 203;

    public static final int REJECT_ALREADY_HAS_LEADER = 204;

    public static final int REJECT_EXPIRED_TERM = 205;

    public static final int REJECT_TERM_NOT_READY = 206;

}
