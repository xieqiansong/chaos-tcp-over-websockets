package lan.chaos.modules.tcp.over.websockets.benchmark;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * 纯网络通讯用的 Echo 服务端：收到什么原样回什么，不含任何隧道/转发逻辑。
 * 从 {@code TunnelRealWorldBenchmarkTest#startEcho} 抽离出来，用于脱离隧道、单独验证
 * TCP / Netty 网络通讯本身（吞吐、RTT、连不通等场景），由调用方自行手动测试。
 *
 * <p>运行方式：
 * <pre>
 *   mvn -pl chaos-tcp-over-websockets-benchmark -am compile exec:java \
 *       -Dexec.mainClass=lan.chaos.modules.tcp.over.websockets.benchmark.EchoServer \
 *       -Dexec.args="20001"
 * </pre>
 * 不传端口时默认监听 {@code 20001}。
 */
@Slf4j
public class EchoServer {

    private final int port;
    private EventLoopGroup group;
    private Channel channel;

    public EchoServer(int port) {
        this.port = port;
    }

    /**
     * 绑定端口并启动监听，绑定完成后立即返回（非阻塞），真正的关闭由 {@link #stop()} 触发。
     * 手动常驻运行时请配合 {@link #await()} 阻塞等待。
     */
    public void start() throws InterruptedException {
        group = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(group).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                // retain 交出的引用：SimpleChannelInboundHandler 会在返回后 release 入站引用，
                                // 这里先 retain 再 writeAndFlush，保证 flush 时底层内存仍有效
                                ctx.writeAndFlush(msg.retain());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ctx.close();
                            }
                        });
                    }
                });
        channel = b.bind(port).sync().channel();
        log.info("EchoServer listening on {}", port);
    }

    /**
     * 阻塞等待服务端关闭（供手动常驻运行时调用）。
     */
    public void await() throws InterruptedException {
        if (channel != null) {
            channel.closeFuture().sync();
        }
    }

    /**
     * 关闭监听并释放 EventLoop 资源。
     */
    public void stop() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0].trim()) : 20001;
        EchoServer server = new EchoServer(port);
        server.start();
        server.await();
    }
}
