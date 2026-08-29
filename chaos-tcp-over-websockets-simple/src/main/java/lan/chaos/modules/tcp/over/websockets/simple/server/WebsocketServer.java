package lan.chaos.modules.tcp.over.websockets.simple.server;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import lan.chaos.modules.tcp.over.websockets.simple.SharedEventLoopGroups;
import lan.chaos.modules.tcp.over.websockets.simple.bufcopy.BufCopyStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Component
@Profile("server")
public class WebsocketServer implements Closeable {
    private final BufCopyStrategy bufCopyStrategy;
    private final Lock lock = new ReentrantLock();

    public WebsocketServer(BufCopyStrategy bufCopyStrategy) {
        this.bufCopyStrategy = bufCopyStrategy;
        SharedEventLoopGroups.acquire(); // 共享 boss/worker group，引用计数 +1
    }


    public void start(int port) {
        log.info("Websocket Server start......");
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(SharedEventLoopGroups.boss(), SharedEventLoopGroups.worker())
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            socketChannel.pipeline()
                                    .addLast(new HttpServerCodec())
//                                    .addLast(new LoggingHandler(LogLevel.INFO))
                                    .addLast(new HttpObjectAggregator(8 * 1024 * 1024))
                                    .addLast(new WebsocketServerHandler(bufCopyStrategy));
                        }
                    });
            ServerBootstrap ignored = SystemUtil.getOsInfo().isWindows() ? bootstrap.channel(NioServerSocketChannel.class) : bootstrap.channel(EpollServerSocketChannel.class);
            ChannelFuture channelFuture = bootstrap.bind(port);
            channelFuture.addListener((ChannelFutureListener) future -> log.info("websocket bind port：{}", port));
            Channel channel = channelFuture.channel();
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
            SharedEventLoopGroups.release(); // 共享 group，引用计数 -1，归零才真正关闭
        } finally {
            lock.unlock();
        }
    }
}