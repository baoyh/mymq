package bao.study.mymq.remoting.netty;

import bao.study.mymq.remoting.InvokeCallback;
import bao.study.mymq.remoting.RemotingClient;
import bao.study.mymq.remoting.RemotingException;
import bao.study.mymq.remoting.RemotingHelper;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.netty.codec.kryo.KryoNettyDecode;
import bao.study.mymq.remoting.netty.codec.kryo.KryoNettyEncode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author baoyh
 * @since 2022/5/13 15:09
 */
public class NettyClient extends NettyAbstract implements RemotingClient {

    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);

    private final NioEventLoopGroup eventLoopGroupWorker = new NioEventLoopGroup();
    private final Bootstrap bootstrap = new Bootstrap();

    private final ConcurrentMap<String, Channel> channelTables = new ConcurrentHashMap<>();
    private final Lock lockChannelTables = new ReentrantLock();
    private final static int LOCK_TIME = 3000;

    @Override
    public void start() {
        this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new KryoNettyDecode()).addLast(new KryoNettyEncode()).addLast(new ClientHandler());
            }
        });
    }

    @Override
    public void shutdown() {
        this.channelTables.clear();
        this.eventLoopGroupWorker.shutdownGracefully();
    }

    @Override
    public void invokeOneway(String address, RemotingCommand request, long timeoutMillis) {
        Channel channel = getOrCreateChannel(address);
        channel.writeAndFlush(request).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                log.warn("send message fail to " + address);
            }
        });
    }

    @Override
    public RemotingCommand invokeSync(String address, RemotingCommand request, long timeoutMillis) {
        Channel channel = getOrCreateChannel(address);
        ResponseFuture responseFuture = new ResponseFuture();
        responseFutureTable.put(request.getRequestId(), responseFuture);
        channel.writeAndFlush(request);
        return responseFuture.awaitResponse(timeoutMillis);
    }

    @Override
    public void invokeAsync(String address, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {
        Channel channel = getOrCreateChannel(address);
        ResponseFuture responseFuture = new ResponseFuture();
        responseFuture.setInvokeCallback(invokeCallback);
        responseFutureTable.put(request.getRequestId(), responseFuture);
        channel.writeAndFlush(request);
    }

    @Override
    public List<String> getRouterAddressList() {
        return null;
    }

    @Override
    public void updateRouterAddressList(List<String> addressList) {

    }

    private Channel getOrCreateChannel(String address) {
        Channel channel = channelTables.get(address);
        if (channel != null) {
            return channel;
        }

        try {
            while (true) {
                if (lockChannelTables.tryLock(LOCK_TIME, TimeUnit.MILLISECONDS)) {
                    channel = channelTables.get(address);
                    if (channel != null) {
                        return channel;
                    }

                    channel = bootstrap.connect(RemotingHelper.string2SocketAddress(address)).sync().channel();
                    channelTables.put(address, channel);
                    return channel;
                }
            }
        } catch (Exception e) {
            throw new RemotingException("create channel fail", e);
        } finally {
            lockChannelTables.unlock();
        }
    }

    @ChannelHandler.Sharable
    class ClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            log.info("[Remote] : " + ctx.channel().remoteAddress() + ", [RemotingCommand] : " + msg);
            processRemotingCommand(ctx, msg);
        }
    }
}
