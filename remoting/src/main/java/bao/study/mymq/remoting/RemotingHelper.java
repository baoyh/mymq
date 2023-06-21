package bao.study.mymq.remoting;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author baoyh
 * @since 2022/5/24 9:51
 */
public abstract class RemotingHelper {

    private static final AttributeKey<String> REMOTE_ADDR_KEY = AttributeKey.valueOf("RemoteAddr");

    public static SocketAddress string2SocketAddress(String address) {
        int split = address.lastIndexOf(":");
        String host = address.substring(0, split);
        String port = address.substring(split + 1);
        return new InetSocketAddress(host, Integer.parseInt(port));
    }

    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }
        Attribute<String> att = channel.attr(REMOTE_ADDR_KEY);
        if (att == null) {
            // mocked in unit test
            return parseChannelRemoteAddr0(channel);
        }
        String addr = att.get();
        if (addr == null) {
            addr = parseChannelRemoteAddr0(channel);
            att.set(addr);
        }
        return addr;
    }

    private static String parseChannelRemoteAddr0(final Channel channel) {
        SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    public static String parseSocketAddressAddr(SocketAddress socketAddress) {
        if (socketAddress != null) {
            final String addr = socketAddress.toString();
            int index = addr.lastIndexOf("/");
            return (index != -1) ? addr.substring(index + 1) : addr;
        }
        return "";
    }
}
