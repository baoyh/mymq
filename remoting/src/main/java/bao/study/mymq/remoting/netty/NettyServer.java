package bao.study.mymq.remoting.netty;

import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.netty.codec.kryo.KryoNettyDecode;
import bao.study.mymq.remoting.netty.codec.kryo.KryoNettyEncode;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author baoyh
 * @since 2022/5/13 15:09
 */
public class NettyServer extends NettyAbstract implements RemotingServer {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    private NioEventLoopGroup bossGroup = new NioEventLoopGroup();
    private NioEventLoopGroup workerGroup = new NioEventLoopGroup();
    private final ServerHandler serverHandler = new ServerHandler();

    private final int port;

    public NettyServer(int port) {
        this.port = port;
    }

    @Override
    public void start() {
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup();

            serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {

                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new KryoNettyDecode()).addLast(new KryoNettyEncode()).addLast(serverHandler);
                }
            });
            serverBootstrap.bind(port).sync();
            hasStarted.compareAndSet(false, true);
            log.info("Success start netty server in port {}", port);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        hasStarted.compareAndSet(true, false);
    }



    @ChannelHandler.Sharable
    class ServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
//            log.info("[Remote] : " + ctx.channel().remoteAddress() + ", [RemotingCommand] : " + msg);
            processRemotingCommand(ctx, msg);
        }
    }

}
