package bao.study.mymq.remoting;

import java.util.List;

/**
 * @author baoyh
 * @since 2022/5/13 15:14
 */
public interface RemotingClient extends Remoting {

    List<String> getRouterAddressList();

    void updateRouterAddressList(List<String> addressList);
}
