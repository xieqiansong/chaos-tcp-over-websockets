package lan.chaos.modules.tcp.over.websockets.multiplexed.server;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import lan.chaos.modules.tcp.over.websockets.multiplexed.session.SessionManager;
import lan.chaos.modules.tcp.over.websockets.multiplexed.util.SharedEventLoopGroups;
import lombok.extern.slf4j.Slf4j;

/**
 * multiplexed Server（骨架）。
 * <p>
 * 监听端口，完成 WebSocket 升级握手，并处理会话建立控制消息（open → 返回 sessionId）。
 * 使用共享 EventLoopGroup（{@link SharedEventLoopGroups}），后续步骤再补充数据转发、多会话复用等。
 */
@Slf4j
public class Server {

    private final SessionManager sessionManager = new SessionManager();

    public Server() {
        SharedEventLoopGroups.acquire(); // 共享 boss/worker group，引用计数 +1
    }

    public void start(int port) {
        log.info("multiplexed Server start, port={}", port);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(SharedEventLoopGroups.boss(), SharedEventLoopGroups.worker())
                    .channel(SystemUtil.getOsInfo().isWindows()
                            ? NioServerSocketChannel.class
                            : EpollServerSocketChannel.class)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new HttpServerCodec())
                                    .addLast(new HttpObjectAggregator(8 * 1024 * 1024))
                                    .addLast(new WebSocketUpgradeHandler(sessionManager));
                        }
                    });

            Channel channel = bootstrap.bind(port).sync().channel();
            log.info("multiplexed Server 已绑定端口: {}", port);
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("multiplexed Server 启动被中断: ", e);
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        SharedEventLoopGroups.release(); // 共享 group，引用计数 -1，归零才真正关闭
    }

    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7002;
        new Server().start(port);
    }
}
