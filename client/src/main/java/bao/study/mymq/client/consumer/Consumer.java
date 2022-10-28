package bao.study.mymq.client.consumer;

/**
 * @author baoyh
 * @since 2022/10/27 10:39
 */
public interface Consumer {

    void subscribe(String topic);

    void registerMessageListener(MessageListener messageListener);
}
