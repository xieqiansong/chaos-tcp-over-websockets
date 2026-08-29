package lan.chaos.modules.tcp.over.websockets.benchmark;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
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

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 真实隧道端到端场景测试（方案 B）：在同一 JVM 内用代码启动三角色 —
 * echo 后端(Netty TCP，收到即回) + 隧道 server(WebsocketServer) + 隧道 client 入口(TcpServer)，
 * 再用一个普通 Socket 充当「数据源」往 client 入口灌流，统计端到端吞吐与往返延迟。
 * <p>
 * 关键设计：<b>每次运行只测一个「策略 × 包大小」组合 + 一个直连对照</b>，通过系统属性指定，
 * 避免连续跑多种组合时前面轮次的线程/端口/引用计数状态污染后续数据（连续混跑会导致
 * retained 等在 256KB/4MB 大包下偶发崩溃、结果不可复现）。
 * <p>
 * 单次运行示例（在仓库根目录执行）：
 * <pre>
 *   # 只测 retained + 256KB
 *   mvn -pl chaos-tcp-over-websockets-benchmark -am test \
 *       -Dtest=TunnelRealWorldBenchmarkTest#runSingle \
 *       -Dbench.strategy=retained -Dbench.payload=262144
 *
 *   # 只测 copied，跑默认包大小（1KB）
 *   mvn -pl chaos-tcp-over-websockets-benchmark -am test \
 *       -Dtest=TunnelRealWorldBenchmarkTest#runSingle -Dbench.strategy=copied
 * </pre>
 * <p>
 * 要在多个策略/包大小间对比，请在命令行逐个单跑，不要在一次 JVM 内连续混跑。
 */
public class TunnelRealWorldBenchmarkTest {

    // ---- 单次组合控制：通过 -Dbench.strategy / -Dbench.payload 指定，默认只测 copied + 1KB ----
    private static final String STRATEGY = System.getProperty("bench.strategy", "copied");
    private static final int PAYLOAD = Integer.getInteger("bench.payload", 1024);

    // ---- 标准格式结果输出文件（CSV，追加）。默认写 target/bench-results.log（构建输出目录，gitignore 忽略）----
    private static final String RESULTS_FILE = System.getProperty(
            "bench.outfile", "target" + File.separator + "bench-results.log");
    private static final String RESULTS_HEADER =
            "timestamp,strategy,payloadBytes,tunnelMbPerSec,tunnelRttMs,tunnelReceived,tunnelError,"
                    + "directMbPerSec,directRttMs,directReceived,directError";

    // ---- 打流参数 ----
    private static final int CONN = 1;                 // 并发打流连接数
    private static final int PER_CONN_MB = 4;          // 每连接打流 MB 总量
    private static final long TIMEOUT_MS = 60000;      // 单轮回收等待上限

    // 端口分配：单次运行只起一个隧道，端口固定即可（避免与其它组合冲突）；直连对照端口独立
    private static final int BASE = 20000;
    private static final int ECHO_PORT = BASE + 1;     // echo 后端
    private static final int WS_PORT = BASE + 2;       // 隧道 WS server
    private static final int LOCAL_PORT = BASE + 3;    // 隧道 client 入口
    private static final int DIRECT_PORT = BASE + 10;  // 直连 echo 对照端口

    static class Result {
        final String name;
        int connCount;
        long receivedBytes;
        double mbPerSec;
        long rttMs;
        String error;

        Result(String name) {
            this.name = name;
        }
    }

    private static BufCopyStrategy resolveStrategy(String name) {
        switch (name) {
            case "copied":
                return new CopiedBufferStrategy();
            case "retained":
                return new RetainedDuplicateStrategy();
            case "duplicate":
                return new DuplicateStrategy();
            default:
                throw new IllegalArgumentException("未知策略: " + name
                        + "（可选 copied/retained/duplicate）");
        }
    }

    /**
     * 单次只测一个「策略 × 包大小」组合 + 直连对照，互不干扰。
     */
    @Test
    void runSingle() throws Exception {
        BufCopyStrategy strategy = resolveStrategy(STRATEGY);
        int payload = PAYLOAD;
        String name = STRATEGY;

        System.out.println("\n======== 单次组合: 策略=" + name + ", 包大小=" + payload + "B ("
                + (payload / 1024.0) + "KB) ========");

        // 被测隧道
        Result tunnel = runRound(strategy, name, BASE, payload);
        // 直连 echo 对照（无隧道）
        Result direct = runDirect(DIRECT_PORT, payload);

        System.out.println("-- " + CONN + " 并发连接，每连接 " + PER_CONN_MB + "MB，合计 "
                + (CONN * PER_CONN_MB) + "MB --");
        printResult(tunnel);
        printResult(direct);
        System.out.println("（direct=直连 echo 后端、无隧道，作为隧道固有开销基线）");
        System.out.println("（duplicate 预期 CRASH：共享引用计数导致异步链路释放错乱）");

        // 以标准 CSV 格式追加到结果文件，供程序化解析（不依赖解析控制台输出）
        appendToResultsFile(tunnel, direct);
    }

    /**
     * 追加一次「隧道 + 直连」结果到输出文件（CSV 追加模式，每组合一行，含隧道与 direct 对照）。首次写入先输出表头。
     */
    private void appendToResultsFile(Result tunnel, Result direct) {
        File f = new File(RESULTS_FILE);
        if (f.getParentFile() != null) {
            f.getParentFile().mkdirs();
        }
        String ts = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        boolean firstWrite = !f.exists() || f.length() == 0;
        try (BufferedWriter w = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(f, true), StandardCharsets.UTF_8))) {
            if (firstWrite) {
                w.write(RESULTS_HEADER);
                w.newLine();
            }
            w.write(csvRow(ts, tunnel, direct));
            w.newLine();
        } catch (IOException e) {
            System.err.println("[bench] 写入结果文件失败 " + RESULTS_FILE + ": " + e.getMessage());
        }
        System.out.println("[bench] 结果已追加到 " + f.getAbsolutePath());
    }

    /**
     * 单行 CSV：一行包含「隧道结果 + 直连对照」，数值 "%.2f"，错误字段为空字符串。
     * 字段顺序与 RESULTS_HEADER 一致。
     */
    private String csvRow(String ts, Result tunnel, Result direct) {
        return String.join(",",
                ts,
                tunnel.name,
                String.valueOf(PAYLOAD),
                fmtNum(tunnel.mbPerSec),
                fmtNum(tunnel.rttMs),
                String.valueOf(tunnel.receivedBytes),
                tunnel.error == null ? "" : csvEscape(tunnel.error),
                fmtNum(direct.mbPerSec),
                fmtNum(direct.rttMs),
                String.valueOf(direct.receivedBytes),
                direct.error == null ? "" : csvEscape(direct.error));
    }

    private String fmtNum(double v) {
        return String.format("%.2f", v);
    }

    private String csvEscape(String s) {
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    private void printResult(Result r) {
        if (r.error != null) {
            System.out.printf("  %-9s CRASH: %s%n", r.name, r.error);
        } else {
            String note = "direct".equals(r.name) ? " (直连echo无隧道)" : "";
            System.out.printf("  %-9s %8.2f MB/s  received=%d(%d连接)  rtt=%d ms%s%n",
                    r.name, r.mbPerSec, r.receivedBytes, r.connCount, r.rttMs, note);
        }
    }

    /**
     * 直连 echo 对照组：仅起 echo 后端，打流客户端直接连 echoPort，不建隧道。
     */
    private Result runDirect(int echoPort, int payload) throws Exception {
        EventLoopGroup echoGroup = startEcho(echoPort);
        try {
            Thread.sleep(500); // 等待绑定
            return runTraffic("127.0.0.1", echoPort, "direct", payload);
        } finally {
            echoGroup.shutdownGracefully();
        }
    }

    /**
     * 起 echo 后端（收到即原样回），返回其 EventLoopGroup 供调用方关闭。
     */
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

    /**
     * 并发打流：CONN 条连接同时连 host:port，每连接发 PER_CONN_MB（包大小 payload），统计端到端吞吐/RTT。
     */
    private Result runTraffic(String host, int port, String name, int payload) throws Exception {
        Result r = new Result(name);
        r.connCount = CONN;
        int roundsPerConn = PER_CONN_MB * 1024 * 1024 / payload;
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
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
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

    /**
     * 起一个完整隧道（echo + WS server + TcpServer 入口），打流，最后清理。
     */
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
            try {
                tcpServer.close();
            } catch (Exception ignore) {
            }
            try {
                ws.close();
            } catch (Exception ignore) {
            }
            echoGroup.shutdownGracefully().syncUninterruptibly();
            Thread.sleep(1500);
        }
        return r;
    }
}
