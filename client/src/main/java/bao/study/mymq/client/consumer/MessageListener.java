package bao.study.mymq.client.consumer;

import bao.study.mymq.common.protocol.MessageExt;

import java.util.List;

/**
 * @author baoyh
 * @since 2022/10/27 10:59
 */
public interface MessageListener {

    ConsumeConcurrentlyStatus consumerMessage(List<MessageExt> messages);
}
