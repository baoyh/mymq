package bao.study.mymq.client.producer;

/**
 * @author baoyh
 * @since 2022/8/2 14:01
 */
public interface SendCallback {

    void onSuccess(final SendResult sendResult);

    void onException(final Throwable e);
}
