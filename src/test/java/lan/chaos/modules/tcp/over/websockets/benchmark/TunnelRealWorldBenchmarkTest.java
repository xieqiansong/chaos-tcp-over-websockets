package lan.chaos.modules.tcp.over.websockets.benchmark;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
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

    private static final int PAYLOAD = 1024;     // 每次发送 1KB
    private static final int ROUNDS = 2000;       // 总打流 2MB
    private static final long TIMEOUT_MS = 20000; // 单轮回收等待上限

    static class Result {
        final String name;
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
        Result[] results = new Result[strategies.size()];
        int base = 20000;
        for (int i = 0; i < strategies.size(); i++) {
            String n = i == 0 ? "copied" : i == 1 ? "retained" : "duplicate";
            results[i] = runRound(strategies.get(i), n, base + i * 100);
        }

        System.out.println("\n==== 真实隧道端到端（echo 后端，2MB 打流） ====");
        for (Result r : results) {
            if (r.error != null) {
                System.out.printf("  %-9s CRASH: %s%n", r.name, r.error);
            } else {
                System.out.printf("  %-9s %8.2f MB/s  received=%d  rtt=%d ms%n",
                        r.name, r.mbPerSec, r.receivedBytes, r.rttMs);
            }
        }
        System.out.println("（duplicate 预期 CRASH：共享引用计数导致异步链路释放错乱）");
    }

    private Result runRound(BufCopyStrategy strategy, String name, int base) throws Exception {
        int echoPort = base + 1, wsPort = base + 2, localPort = base + 3;
        Result r = new Result(name);

        // 1) echo 后端：收到即原样回
        EventLoopGroup echoGroup = new NioEventLoopGroup();
        ServerBootstrap eb = new ServerBootstrap();
        eb.group(echoGroup).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                ctx.writeAndFlush(msg); // echo 原样回显
                            }
                        });
                    }
                });
        eb.bind(echoPort).sync();

        // 2) 隧道 server（WS 端，转发到 echo）
        WebsocketServer ws = new WebsocketServer(strategy);
        Thread wsThread = new Thread(() -> ws.start(wsPort));
        wsThread.setDaemon(true);
        wsThread.start();

        // 3) 隧道 client 入口（监听到 localPort，连 ws 后转发到 echo）
        TcpServer tcpServer = new TcpServer(strategy);
        String wsUrl = "ws://127.0.0.1:" + wsPort + "/forward/127.0.0.1/" + echoPort;
        Thread clientThread = new Thread(() -> tcpServer.start(localPort, wsUrl));
        clientThread.setDaemon(true);
        clientThread.start();

        Thread.sleep(1000); // 等待各端口绑定

        try (Socket s = new Socket("127.0.0.1", localPort)) {
            s.setSoTimeout(3000);
            java.io.OutputStream out = s.getOutputStream();
            java.io.InputStream in = s.getInputStream();
            byte[] payload = new byte[PAYLOAD];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) i;
            }

            AtomicLong total = new AtomicLong(0);
            AtomicLong first = new AtomicLong(0);
            AtomicLong last = new AtomicLong(0);
            AtomicBoolean stop = new AtomicBoolean(false);
            Thread reader = new Thread(() -> {
                byte[] buf = new byte[8192];
                try {
                    while (!stop.get()) {
                        int n = in.read(buf);
                        if (n < 0) break;
                        long t = System.nanoTime();
                        if (first.get() == 0) first.set(t);
                        last.set(t);
                        total.addAndGet(n);
                    }
                } catch (Exception ignore) { /* socket 关闭 */ }
            });
            reader.setDaemon(true);
            reader.start();

            long start = System.nanoTime();
            for (int i = 0; i < ROUNDS; i++) {
                out.write(payload);
            }
            out.flush();

            long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (total.get() < (long) ROUNDS * PAYLOAD && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            stop.set(true);

            r.receivedBytes = total.get();
            double mb = r.receivedBytes / 1024.0 / 1024.0;
            r.mbPerSec = elapsedMs > 0 ? mb * 1000 / elapsedMs : 0;
            r.rttMs = (last.get() - first.get()) / 1_000_000;
            if (r.receivedBytes < (long) ROUNDS * PAYLOAD * 0.99) {
                r.error = "链路异常：仅回收 " + r.receivedBytes + "/" + ((long) ROUNDS * PAYLOAD) + " 字节";
            }
        } catch (Exception e) {
            r.error = e.getClass().getSimpleName() + ": " + e.getMessage();
        } finally {
            try { tcpServer.close(); } catch (Exception ignore) { }
            try { ws.close(); } catch (Exception ignore) { }
            echoGroup.shutdownGracefully();
        }
        return r;
    }
}
