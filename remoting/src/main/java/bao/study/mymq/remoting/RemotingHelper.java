package bao.study.mymq.remoting;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author baoyh
 * @since 2022/5/24 9:51
 */
public abstract class RemotingHelper {

    public static SocketAddress string2SocketAddress(String address) {
        int split = address.lastIndexOf(":");
        String host = address.substring(0, split);
        String port = address.substring(split + 1);
        return new InetSocketAddress(host, Integer.parseInt(port));
    }
}
