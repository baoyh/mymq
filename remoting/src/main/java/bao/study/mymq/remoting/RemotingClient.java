package bao.study.mymq.remoting;

import bao.study.mymq.remoting.common.RemotingCommand;

import java.util.List;

/**
 * @author baoyh
 * @since 2022/5/13 15:14
 */
public interface RemotingClient extends Remoting {

    void invokeOneway(final String address, final RemotingCommand request, final long timeoutMillis);

    RemotingCommand invokeSync(final String address, final RemotingCommand request, final long timeoutMillis);

    void invokeAsync(final String address, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback);

    List<String> getRouterAddressList();

    void updateRouterAddressList(List<String> addressList);
}
