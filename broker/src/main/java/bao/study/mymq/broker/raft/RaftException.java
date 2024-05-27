package bao.study.mymq.broker.raft;

/**
 *
 * @author baoyh
 * @since 2024/5/22 15:06
 */
public class RaftException extends RuntimeException {

    public RaftException(String message) {
        super(message);
    }
}
