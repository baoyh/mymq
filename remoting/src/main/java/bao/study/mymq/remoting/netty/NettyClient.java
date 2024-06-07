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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author baoyh
 * @since 2022/5/13 15:09
 */
public class NettyClient extends NettyAbstract implements RemotingClient {

    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);

    private NioEventLoopGroup eventLoopGroupWorker = new NioEventLoopGroup();
    private Bootstrap bootstrap = new Bootstrap();

    private final ConcurrentMap<String, Channel> channelTables = new ConcurrentHashMap<>();

    @Override
    public void start() {
        eventLoopGroupWorker = new NioEventLoopGroup();
        bootstrap = new Bootstrap();

        this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new KryoNettyDecode()).addLast(new KryoNettyEncode()).addLast(new ClientHandler());
            }
        });

        hasStarted.compareAndSet(false, true);
    }

    @Override
    public void shutdown() {
        channelTables.forEach((k, v) -> v.close().addListener(future ->
                log.info("CloseChannel: close the connection to remote address[{}] result: {}", RemotingHelper.parseChannelRemoteAddr(v),
                        future.isSuccess())));
        this.channelTables.clear();
        this.eventLoopGroupWorker.shutdownGracefully();
        hasStarted.compareAndSet(true, false);
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
        channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                requestFail(request.getRequestId());
                closeChannel(address);
                log.warn("Send a request command to address {} failed.", address);
            }
        });
        return responseFuture.awaitResponse(timeoutMillis);
    }

    @Override
    public void invokeAsync(String address, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {
        Channel channel = getOrCreateChannel(address);
        ResponseFuture responseFuture = new ResponseFuture();
        responseFuture.setInvokeCallback(invokeCallback);
        responseFutureTable.put(request.getRequestId(), responseFuture);
        channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                requestFail(request.getRequestId());
                closeChannel(address);
                log.warn("Send a request command to address {} failed.", address);
            }
        });
    }

    private void requestFail(int requestId) {
        ResponseFuture responseFuture = responseFutureTable.remove(requestId);
        if (responseFuture == null) return;
        InvokeCallback invokeCallback = responseFuture.getInvokeCallback();
        if (invokeCallback != null) {
            invokeCallback.callback(responseFuture);
        }
        responseFuture.getCountDownLatch().countDown();
    }

    private void closeChannel(String address) {
        Channel channel = channelTables.remove(address);
        if (channel != null) {
            channel.close().addListener(future -> {
                if (future.isSuccess()) {
                    log.info("Close channel close the connection to remote address {}", address);
                }
            });
        }
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
            channel = bootstrap.connect(RemotingHelper.string2SocketAddress(address)).sync().channel();
            channelTables.put(address, channel);
            log.info("Success create channel with address {}", address);
            return channel;
        } catch (Exception e) {
            throw new RemotingException("Create channel fail with address " + address, e);
        }
    }

    @ChannelHandler.Sharable
    class ClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            processRemotingCommand(ctx, msg);
        }
    }
}
