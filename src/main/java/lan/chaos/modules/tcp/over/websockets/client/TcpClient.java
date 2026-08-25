package lan.chaos.modules.tcp.over.websockets.client;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lan.chaos.modules.tcp.over.websockets.chunk.ChunkStrategy;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class TcpClient implements Runnable {
    private final String targetHost;
    private final Integer targetPort;
    private EventLoopGroup workGroup = SystemUtil.getOsInfo().isWindows() ? new NioEventLoopGroup() : new EpollEventLoopGroup();
    private Bootstrap bootstrap = new Bootstrap();
    private ChannelFuture channelFuture;
    private Channel websocketChannel;
    private final BufCopyStrategy bufCopyStrategy;
    private final ChunkStrategy chunkStrategy;

    public TcpClient(String host, Integer port, final Channel channel, BufCopyStrategy bufCopyStrategy, ChunkStrategy chunkStrategy) {
        this.bufCopyStrategy = bufCopyStrategy;
        this.chunkStrategy = chunkStrategy;
        log.info("Tcp Client connect start......");
        this.targetHost = host;
        this.targetPort = port;
        setWebsocketChannel(channel);
        if (this.targetPort == null) {
            log.error("tcp client 初始化失败,端口未找到, 源端口: " + port);
            return;
        }
        Bootstrap ignored = SystemUtil.getOsInfo().isWindows() ? bootstrap.channel(NioSocketChannel.class) : bootstrap.channel(EpollSocketChannel.class);
        bootstrap.group(workGroup)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        log.debug("初始化 channel .... ");
                        ch.pipeline().addLast(new TcpClientHandler(channel, bufCopyStrategy, chunkStrategy));
                    }
                });
    }

    public void writeAndFlush(ByteBuf msg) {
        channelFuture.channel().writeAndFlush(msg);
    }

    public boolean isClose() {
        return channelFuture == null || !channelFuture.channel().isOpen();
    }

    public void close() {
        try {
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        try {
            channelFuture = bootstrap.connect(targetHost, this.targetPort).sync();
            if (channelFuture.isSuccess()) {
                log.info("建立tcp连接成功, target ip: {}, port {}", this.targetHost, this.targetPort);
            }
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("error: ", e);
        } finally {
            workGroup.shutdownGracefully();
        }
        log.debug("建立连接结束.... ");
    }
}