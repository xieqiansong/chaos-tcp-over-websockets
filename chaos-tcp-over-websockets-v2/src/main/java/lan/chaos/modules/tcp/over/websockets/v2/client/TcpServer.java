package lan.chaos.modules.tcp.over.websockets.v2.client;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lan.chaos.modules.tcp.over.websockets.v2.util.SharedEventLoopGroups;
import lombok.extern.slf4j.Slf4j;

/**
 * v2 Client 端本地 TCP Server（骨架）。
 * <p>
 * 监听本地端口，接受 TCP 连接。每个 TCP 连接进来后，
 * 通过已建立的 WebSocket 连接向 server 发送第一条控制消息（open，携带目标地址），
 * 收到 server 返回的 sessionId 后暂存（数据转发留待后续步骤）。
 */
@Slf4j
public class TcpServer {

    private final Client client;

    public TcpServer(Client client) {
        this.client = client;
        SharedEventLoopGroups.acquire(); // 共享 boss/worker group，引用计数 +1
    }

    public void start(int port, String targetHost, int targetPort) {
        log.info("v2 TcpServer start, port={}", port);
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
                            ch.pipeline().addLast(new TcpServerHandler(client, targetHost, targetPort));
                        }
                    });

            Channel channel = bootstrap.bind(port).sync().channel();
            log.info("v2 TcpServer 已绑定端口: {}", port);
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("v2 TcpServer 启动被中断: ", e);
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        SharedEventLoopGroups.release();
    }

    public static void main(String[] args) {
        String wsUrl = args.length > 0 ? args[0] : "ws://localhost:7002";

        Client client = new Client();
        client.connect(wsUrl);

        TcpServer tcpServer = new TcpServer(client);
        // 阻塞监听本地端口
        tcpServer.start(21002, "127.0.0.1", 20001);
    }
}
