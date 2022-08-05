package bao.study.mymq.client.producer;

/**
 * @author baoyh
 * @since 2022/8/2 13:28
 */
public enum SendStatus {
    SEND_OK,
    MASTER_NOT_AVAILABLE,
    SEND_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT
}
