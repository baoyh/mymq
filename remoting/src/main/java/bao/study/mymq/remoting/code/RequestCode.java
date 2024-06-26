package bao.study.mymq.remoting.code;

/**
 * @author baoyh
 * @since 2022/5/13 14:55
 */
public abstract class RequestCode {

    /**
     * message
     */
    public static final int SEND_MESSAGE = 100;

    public static final int PULL_MESSAGE = 101;


    /**
     * broker
     */
    public static final int REGISTER_BROKER = 200;

    public static final int UNREGISTER_BROKER = 201;

    public static final int BROKER_HEARTBEAT = 202;

    public static final int FLUSH_SYNC = 203;

    public static final int REGISTER_MASTER = 204;


    /**
     * client
     */
    public static final int GET_ROUTE_BY_TOPIC = 300;

    public static final int QUERY_CONSUMER_OFFSET = 301;

    public static final int CONSUMER_SEND_MSG_BACK = 302;

    public static final int QUERY_ALIVE_BROKERS = 303;

    public static final int REGISTER_CONSUMER = 304;

    public static final int QUERY_CONSUMERS_BY_GROUP = 305;


    /**
     * raft
     */
    public static final int SEND_HEARTBEAT = 400;

    public static final int CALL_VOTE = 401;

    public static final int APPEND = 402;

    public static final int PUSH = 403;

}
