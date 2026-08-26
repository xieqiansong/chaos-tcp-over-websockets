package lan.chaos.modules.tcp.over.websockets.client;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Component
@Profile("client")
public class TcpServer implements Closeable {
    private final BufCopyStrategy bufCopyStrategy;
    private final EventLoopGroup bossGroup = SystemUtil.getOsInfo().isWindows() ? new NioEventLoopGroup() : new EpollEventLoopGroup();
    private final EventLoopGroup workGroup = SystemUtil.getOsInfo().isWindows() ? new NioEventLoopGroup() : new EpollEventLoopGroup();
    private final Lock lock = new ReentrantLock();

    public TcpServer(BufCopyStrategy bufCopyStrategy) {
        this.bufCopyStrategy = bufCopyStrategy;
    }


    public void start(int port, String wsUrl) {
        log.info("Tcp Server start......");
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(bossGroup, workGroup)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    // 默认 AdaptiveRecvByteBufAllocator 上限 64KB 会把大 TCP 数据块切成多个 64KB 块转发，
                    // 这里把上限调大到 8MB，与 WS 帧上限(8MB)匹配，减少转发/flush 次数（每包固定开销）
                    .childOption(ChannelOption.RCVBUF_ALLOCATOR,
                            new AdaptiveRecvByteBufAllocator(64, 1 * 1024 * 1024, 8 * 1024 * 1024))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new TcpServerHandler(wsUrl, bufCopyStrategy))
                            ;
                        }
                    });
            if (SystemUtil.getOsInfo().isWindows()) {
                bootstrap.channel(NioServerSocketChannel.class);
            } else {
                bootstrap.channel(EpollServerSocketChannel.class);
            }
            ChannelFuture channelFuture = bootstrap.bind(port);
            channelFuture.addListener((ChannelFutureListener) channelFuture1 -> log.info("tcp 成功绑定端口, " + port));
            final Channel channel = channelFuture.channel();
            channel.closeFuture().addListener(future -> channel.close());
            channel.closeFuture().sync();
        } catch (Exception e) {
            log.error("error: ", e);
        } finally {
            close();
        }

    }

    @Override
    @PreDestroy
    public void close() {
        lock.lock();
        try {
            if (bossGroup.isShutdown() && workGroup.isShutdown()) {
                return;
            }
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        } finally {
            lock.unlock();
        }
    }
}