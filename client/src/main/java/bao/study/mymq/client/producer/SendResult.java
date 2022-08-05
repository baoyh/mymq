package bao.study.mymq.client.producer;

import bao.study.mymq.common.protocol.message.MessageQueue;

/**
 * @author baoyh
 * @since 2022/8/2 13:26
 */
public class SendResult {

    private SendStatus sendStatus;

    private MessageQueue messageQueue;

    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }
}
