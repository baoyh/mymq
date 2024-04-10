package bao.study.mymq.broker.raft.protocol;

import bao.study.mymq.common.protocol.raft.HeartBeat;

/**
 * @author baoyh
 * @since 2024/4/9 13:51
 */
public interface ServerProtocol {

    HeartBeat handleHeartbeat(HeartBeat heartBeat);
}
