package bao.study.mymq.remoting;

import bao.study.mymq.remoting.netty.ResponseFuture;

/**
 * @author baoyh
 * @since 2022/5/24 13:39
 */
public interface InvokeCallback {

    void callback(ResponseFuture responseFuture);
}
