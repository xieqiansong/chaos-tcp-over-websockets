package lan.chaos.modules.tcp.over.websockets.client;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lan.chaos.modules.tcp.over.websockets.SharedEventLoopGroups;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class TcpClient {
    private final String targetHost;
    private final Integer targetPort;
    private Bootstrap bootstrap = new Bootstrap();
    private ChannelFuture channelFuture;
    private Channel websocketChannel;
    private final BufCopyStrategy bufCopyStrategy;

    public TcpClient(String host, Integer port, final Channel channel, BufCopyStrategy bufCopyStrategy) {
        this.bufCopyStrategy = bufCopyStrategy;
        SharedEventLoopGroups.acquire(); // 共享 worker group，引用计数 +1
        log.info("Tcp Client connect start......");
        this.targetHost = host;
        this.targetPort = port;
        setWebsocketChannel(channel);
        if (this.targetPort == null) {
            log.error("tcp client 初始化失败,端口未找到, 源端口: " + port);
            SharedEventLoopGroups.release();
            return;
        }
        Bootstrap ignored = SystemUtil.getOsInfo().isWindows() ? bootstrap.channel(NioSocketChannel.class) : bootstrap.channel(EpollSocketChannel.class);
        bootstrap.group(SharedEventLoopGroups.worker())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        log.debug("初始化 channel .... ");
                        ch.pipeline().addLast(new TcpClientHandler(channel, bufCopyStrategy));
                    }
                });
        // 构造时同步建立到目标 echo 的连接（与 WebsocketClient 一致），
        // 避免依赖外部固定线程池执行 run()。连接建立后由 EventLoop 管理生命周期，无需驻留线程。
        try {
            channelFuture = bootstrap.connect(targetHost, this.targetPort).sync();
            if (channelFuture.isSuccess()) {
                log.info("建立tcp连接成功, target ip: {}, port {}", this.targetHost, this.targetPort);
            }
        } catch (InterruptedException e) {
            log.error("error: ", e);
            Thread.currentThread().interrupt();
        }
    }

    public void writeAndFlush(ByteBuf msg) {
        channelFuture.channel().writeAndFlush(msg);
    }

    public boolean isClose() {
        return channelFuture == null || !channelFuture.channel().isOpen();
    }

    public void close() {
        // 非阻塞关闭：原实现 closeFuture().sync() 在 EventLoop 线程（handlerRemoved 回调）调用会阻塞，
        // 改为直接关闭通道，连接关闭由 EventLoop 异步完成，避免死锁/卡住。
        ChannelFuture cf = channelFuture;
        if (cf != null && cf.channel() != null) {
            cf.channel().close();
        }
        SharedEventLoopGroups.release(); // 共享 group，引用计数 -1，归零才真正关闭
    }
}