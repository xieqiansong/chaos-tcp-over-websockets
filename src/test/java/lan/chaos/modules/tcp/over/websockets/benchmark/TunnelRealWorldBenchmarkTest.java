package lan.chaos.modules.tcp.over.websockets.benchmark;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lan.chaos.modules.tcp.over.websockets.bufcopy.CopiedBufferStrategy;
import lan.chaos.modules.tcp.over.websockets.bufcopy.DuplicateStrategy;
import lan.chaos.modules.tcp.over.websockets.bufcopy.RetainedDuplicateStrategy;
import lan.chaos.modules.tcp.over.websockets.client.TcpServer;
import lan.chaos.modules.tcp.over.websockets.server.WebsocketServer;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 真实隧道端到端场景测试（方案 B）：在同一 JVM 内用代码启动三角色 —
 *   echo 后端(Netty TCP，收到即回) + 隧道 server(WebsocketServer) + 隧道 client 入口(TcpServer)，
 * 再用一个普通 Socket 充当「数据源」往 client 入口灌流，统计端到端吞吐与往返延迟。
 *
 * 三轮分别注入 copied / retained / duplicate 三策略（直接 new 注入，绕过 BufCopyConfiguration 的
 * duplicate 回退），真实验证零拷贝在异步转发链路上的收益与 duplicate 的链路不安全。
 *
 * IDEA 中打开本文件 → 点左侧绿色箭头 Run 即跑完整三轮，无需命令。
 */
public class TunnelRealWorldBenchmarkTest {

    // 打流包大小维度：1KB(小包) / 32KB / 1MB / 16MB(超大)。每尺寸每连接仍打 PER_CONN_MB 总量。
    private static final int[] PAYLOADS = {1024, 32 * 1024, 1024 * 1024, 16 * 1024 * 1024};
    private static final int CONN = 1;                // 并发打流连接数（临时降为1验证并发是否为 retained 崩溃根因）
    private static final int PER_CONN_MB = 16;         // 每连接打流 16MB
    private static final long TIMEOUT_MS = 60000;      // 单轮回收等待上限(并发放大)

    static class Result {
        final String name;
        int connCount;
        long receivedBytes;
        double mbPerSec;
        long rttMs;
        String error;
        Result(String name) { this.name = name; }
    }

    @Test
    void realWorldThreeStrategies() throws Exception {
        List<BufCopyStrategy> strategies = Arrays.asList(
                new CopiedBufferStrategy(),
                new RetainedDuplicateStrategy(),
                new DuplicateStrategy());

        int base = 20000;
        int directBase = 21000;
        for (int pi = 0; pi < PAYLOADS.length; pi++) {
            int payload = PAYLOADS[pi];
            System.out.println("\n======== 包大小 " + payload + "B (" + (payload / 1024) + "KB) ========");

            Result[] results = new Result[strategies.size()];
            for (int i = 0; i < strategies.size(); i++) {
                String n = i == 0 ? "copied" : i == 1 ? "retained" : "duplicate";
                results[i] = runRound(strategies.get(i), n, base + i * 100, payload);
            }
            // 直连 echo 对照组：打流客户端直接连 echo 后端，不经过隧道，作为隧道开销基线
            results[results.length - 1] = runDirect(directBase + pi * 10, payload);

            System.out.println("-- " + CONN + " 并发连接，每连接 " + PER_CONN_MB + "MB，合计 "
                    + (CONN * PER_CONN_MB) + "MB --");
            for (Result r : results) {
                if (r.error != null) {
                    System.out.printf("  %-9s CRASH: %s%n", r.name, r.error);
                } else {
                    String note = "direct".equals(r.name) ? " (直连echo无隧道)" : "";
                    System.out.printf("  %-9s %8.2f MB/s  received=%d(%d连接)  rtt=%d ms%s%n",
                            r.name, r.mbPerSec, r.receivedBytes, r.connCount, r.rttMs, note);
                }
            }
        }
        System.out.println("（duplicate 预期 CRASH：共享引用计数导致异步链路释放错乱；rtt 为并发下首末包跨度，仅参考）");
        System.out.println("（direct=直连 echo 后端、无隧道，作为隧道固有开销基线；对比各包大小可观察包大小对吞吐的影响）");
    }

    /** 直连 echo 对照组：仅起 echo 后端，打流客户端直接连 echoPort，不建隧道。 */
    private Result runDirect(int echoPort, int payload) throws Exception {
        EventLoopGroup echoGroup = startEcho(echoPort);
        try {
            Thread.sleep(500); // 等待绑定
            return runTraffic("127.0.0.1", echoPort, "direct", payload);
        } finally {
            echoGroup.shutdownGracefully();
        }
    }

    /** 起 echo 后端（收到即原样回），返回其 EventLoopGroup 供调用方关闭。 */
    private EventLoopGroup startEcho(int echoPort) throws Exception {
        EventLoopGroup echoGroup = new NioEventLoopGroup();
        ServerBootstrap eb = new ServerBootstrap();
        eb.group(echoGroup).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ctx.writeAndFlush(msg.retain()); // retain 交出的引用，Simple 会在返回后 release 入站引用
                            }
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ctx.close();
                            }
                        });
                    }
                });
        eb.bind(echoPort).sync();
        return echoGroup;
    }

    /** 并发打流：CONN 条连接同时连 host:port，每连接发 PER_CONN_MB（包大小 payload），统计端到端吞吐/RTT。 */
    private Result runTraffic(String host, int port, String name, int payload) throws Exception {
        Result r = new Result(name);
        r.connCount = CONN;
        int roundsPerConn = PER_CONN_MB * 1024 * 1024 / payload; // 保持每连接总量 16MB
        long expected = (long) CONN * roundsPerConn * payload;
        AtomicLong total = new AtomicLong(0);
        AtomicLong first = new AtomicLong(0);
        AtomicLong last = new AtomicLong(0);
        CountDownLatch gate = new CountDownLatch(1);     // 发令枪：保证多连接同时起步
        CountDownLatch done = new CountDownLatch(CONN);
        byte[] pl = new byte[payload];
        for (int i = 0; i < pl.length; i++) {
            pl[i] = (byte) i;
        }

        for (int c = 0; c < CONN; c++) {
            new Thread(() -> {
                try (Socket s = new Socket(host, port)) {
                    s.setSoTimeout(5000);
                    java.io.OutputStream out = s.getOutputStream();
                    java.io.InputStream in = s.getInputStream();
                    byte[] rbuf = new byte[8192];
                    gate.await();                        // 等发令，多连接同时开打
                    for (int i = 0; i < roundsPerConn; i++) {
                        out.write(pl);
                        // 边写边读回包，避免发送端 TCP 缓冲阻塞
                        int need = payload;
                        while (need > 0) {
                            int n = in.read(rbuf);
                            if (n < 0) break;
                            long t = System.nanoTime();
                            if (first.get() == 0) first.set(t);
                            last.set(t);
                            total.addAndGet(n);
                            need -= n;
                        }
                    }
                } catch (Exception e) {
                    r.error = "连接异常: " + e.getClass().getSimpleName();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        long start = System.nanoTime();
        gate.countDown();                                // 发令
        try {
            done.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) { Thread.currentThread().interrupt(); }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        r.receivedBytes = total.get();
        double mb = r.receivedBytes / 1024.0 / 1024.0;
        r.mbPerSec = elapsedMs > 0 ? mb * 1000 / elapsedMs : 0;
        r.rttMs = (last.get() - first.get()) / 1_000_000;
        if (r.error == null && r.receivedBytes < expected * 0.99) {
            r.error = "链路异常：仅回收 " + r.receivedBytes + "/" + expected + " 字节";
        }
        return r;
    }

    private Result runRound(BufCopyStrategy strategy, String name, int base, int payload) throws Exception {
        int echoPort = base + 1, wsPort = base + 2, localPort = base + 3;
        EventLoopGroup echoGroup = startEcho(echoPort);

        // 隧道 server（WS 端，转发到 echo）
        WebsocketServer ws = new WebsocketServer(strategy);
        Thread wsThread = new Thread(() -> ws.start(wsPort));
        wsThread.setDaemon(true);
        wsThread.start();

        // 隧道 client 入口（监听到 localPort，连 ws 后转发到 echo）
        TcpServer tcpServer = new TcpServer(strategy);
        String wsUrl = "ws://127.0.0.1:" + wsPort + "/forward/127.0.0.1/" + echoPort;
        Thread clientThread = new Thread(() -> tcpServer.start(localPort, wsUrl));
        clientThread.setDaemon(true);
        clientThread.start();

        Thread.sleep(1000); // 等待各端口绑定

        Result r;
        try {
            r = runTraffic("127.0.0.1", localPort, name, payload);
        } finally {
            try { tcpServer.close(); } catch (Exception ignore) { }
            try { ws.close(); } catch (Exception ignore) { }
            echoGroup.shutdownGracefully().syncUninterruptibly();
            // 等待 tcpServer/ws 的异步 eventLoop 关闭完成，避免档间端口/线程残留导致后续档崩溃
            Thread.sleep(1500);
        }
        return r;
    }
}
