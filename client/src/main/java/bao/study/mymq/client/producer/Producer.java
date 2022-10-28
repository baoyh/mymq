package bao.study.mymq.client.producer;

import bao.study.mymq.common.protocol.Message;

/**
 * @author baoyh
 * @since 2022/8/2 11:24
 */
public interface Producer {

    SendResult send(final Message message);

    void send(final Message message, final SendCallback sendCallback);

    void sendOneway(final Message message);
}
