package bao.study.mymq.remoting.netty;

import bao.study.mymq.remoting.RemotingServer;
import bao.study.mymq.remoting.common.RemotingCommand;
import bao.study.mymq.remoting.netty.codec.KryoNettyDecode;
import bao.study.mymq.remoting.netty.codec.KryoNettyEncode;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @author baoyh
 * @since 2022/5/13 15:09
 */
public class NettyServer extends NettyAbstract implements RemotingServer {

    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup();
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup();
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final ServerHandler serverHandler = new ServerHandler();
    private ChannelFuture sync;

    private final int port;

    public NettyServer(int port) {
        this.port = port;
    }

    @Override
    public void start() throws InterruptedException {
        serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new KryoNettyEncode()).addLast(new KryoNettyDecode()).addLast(serverHandler);
            }
        });

        sync = this.serverBootstrap.bind(port).sync();
    }

    @Override
    public void shutdown() throws InterruptedException {
        if (sync != null) {
            sync.channel().closeFuture().sync();
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @ChannelHandler.Sharable
    class ServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            System.out.println(msg);
            processRequest(ctx, msg);
        }
    }

}
