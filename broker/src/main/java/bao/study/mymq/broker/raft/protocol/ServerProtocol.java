package bao.study.mymq.broker.raft.protocol;

/**
 * @author baoyh
 * @since 2024/4/9 13:51
 */
public interface ServerProtocol {

    void handleHeartbeats();
}
