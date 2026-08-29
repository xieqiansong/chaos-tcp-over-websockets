package lan.chaos.modules.tcp.over.websockets.multiplexed.client;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.ControlMessage;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.ControlMessageCodec;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.DataFrameCodec;
import lan.chaos.modules.tcp.over.websockets.multiplexed.util.SharedEventLoopGroups;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * multiplexed Client（WebSocket 连接池）。
 * <p>
 * 维护一组到 server 的 WebSocket 连接（{@link #connect(String, int)} 指定池大小），
 * 每个本地 TCP 会话（open→opened）被分配到其中一条 WS 通道上，后续该会话的所有数据帧都走这条通道。
 * 因此多条 WS 通道互相隔离：某条 WS 通道的抖动/断开只影响落在它上面的会话，不会波及其它通道上的本地 TCP 连接。
 * <p>
 * 会话与 WS 通道的绑定关系见 {@link #sessionToWsChannel}；握手逻辑见 {@link WebSocketClientHandler}。
 * 使用共享 EventLoopGroup（{@link SharedEventLoopGroups}）。
 */
@Slf4j
public class Client {

    /** 默认连接池大小。 */
    public static final int DEFAULT_POOL_SIZE = 4;

    /** WS 连接池（到 server 的多条 WebSocket 通道）。 */
    private final List<Channel> wsPool = new CopyOnWriteArrayList<>();

    /** round-robin 计数器，用于为新会话挑选 WS 通道。 */
    private final AtomicLong rr = new AtomicLong(0);

    /** 本地 TCP 通道 -> sessionId（open 后由 opened 消息回填）。 */
    private final Map<Channel, Long> tcpChannelToSession = new ConcurrentHashMap<>();

    /** sessionId -> 本地 TCP 通道（收到数据帧时按 sessionId 写回）。 */
    private final Map<Long, Channel> sessionToTcpChannel = new ConcurrentHashMap<>();

    /** sessionId -> 承载该会话的 WS 通道（数据帧据此路由）。 */
    private final Map<Long, Channel> sessionToWsChannel = new ConcurrentHashMap<>();

    /** requestId -> 本地 TCP 通道（open 发出后等待 opened 回填）。 */
    private final Map<String, Channel> pendingByRequestId = new ConcurrentHashMap<>();

    /**
     * 本地 TCP 会话尚未 opened 前，暂存本地 TCP 收到的数据 payload。
     * key 存在即表示"仍在等待/flush 缓冲"，后续到达的数据继续入队以保证有序。
     */
    private final Map<Channel, Queue<ByteBuf>> pendingWrites = new ConcurrentHashMap<>();

    private final AtomicLong requestIdSeq = new AtomicLong(0);

    /** 当前 server ws 地址（用于断线重连）。 */
    private URI wsUri;

    /** 目标连接池大小（断线后自动补充到此数量）。 */
    private int targetPoolSize = 1;

    /** 是否已关闭（关闭后停止重连）。 */
    private volatile boolean closed = false;

    /** 专门负责重连调度的线程池，避免使用 Netty event-loop 线程执行阻塞的 connect().sync()。 */
    private final ScheduledExecutorService reconnectExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "multiplexed-client-reconnect");
                t.setDaemon(true);
                return t;
            });

    /** 重连退避计数。 */
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);

    public Client() {
        SharedEventLoopGroups.acquire(); // 共享 worker group，引用计数 +1
    }

    public void connect(String wsUrl) {
        connect(wsUrl, DEFAULT_POOL_SIZE);
    }

    /**
     * 建立指定数量的 WebSocket 连接到 server（连接池），并记住地址与池大小以便后续自动重连。
     *
     * @param wsUrl    server 的 ws 地址
     * @param poolSize 连接池大小，至少为 1
     */
    public void connect(String wsUrl, int poolSize) {
        int n = Math.max(1, poolSize);
        this.targetPoolSize = n;
        try {
            this.wsUri = new URI(wsUrl);
        } catch (URISyntaxException e) {
            log.error("multiplexed Client 非法 wsUrl: {}", wsUrl, e);
            shutdown();
            return;
        }
        log.info("multiplexed Client connect, wsUrl={}, poolSize={}", wsUrl, n);
        try {
            for (int i = 0; i < n; i++) {
                wsPool.add(connectOne(wsUri));
            }
            log.info("multiplexed Client 已建立 {} 条 WS 连接", wsPool.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("multiplexed Client 连接被中断: ", e);
            shutdown();
        }
    }

    private Channel connectOne(URI uri) throws InterruptedException {
        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), 8 * 1024 * 1024);
        WebSocketClientHandler handler = new WebSocketClientHandler(handshaker, this);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(SharedEventLoopGroups.worker())
                .channel(SystemUtil.getOsInfo().isWindows()
                        ? NioSocketChannel.class
                        : EpollSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new HttpClientCodec())
                                .addLast(new HttpObjectAggregator(8 * 1024 * 1024))
                                .addLast(handler);
                    }
                });

        Channel ch = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
        handler.handshakeFuture().sync();
        log.info("multiplexed Client 已建立连接: {}", ch.id().asShortText());
        return ch;
    }

    /** 按 round-robin 从连接池挑选一条 WS 通道。 */
    private Channel nextChannel() {
        if (wsPool.isEmpty()) {
            return null;
        }
        return wsPool.get((int) Math.floorMod(rr.incrementAndGet(), wsPool.size()));
    }

    public void sendText(Channel ws, String text) {
        if (ws != null && ws.isActive()) {
            ws.writeAndFlush(new TextWebSocketFrame(text));
            log.info("multiplexed Client 发送文本: {}", text);
        }
    }

    private void sendClose(Channel ws, long sessionId) {
        ControlMessage close = ControlMessage.close(sessionId);
        sendText(ws, ControlMessageCodec.encode(close));
    }

    /**
     * 向 server 发送 open 控制消息，请求建立会话。
     * 从连接池挑选一条 WS 通道承载该会话；server 返回 opened（含 sessionId + requestId）后由
     * {@link WebSocketClientHandler} 携带对应 WS 通道回填映射。
     *
     * @param tcpChannel 本地 TCP 通道，用于建立 sessionId 映射
     */
    public void openSession(String host, int port, Channel tcpChannel) {
        Channel ws = nextChannel();
        if (ws == null) {
            log.warn("multiplexed Client 无法 open：无可用 WS 连接");
            return;
        }
        String requestId = String.valueOf(requestIdSeq.incrementAndGet());
        pendingByRequestId.put(requestId, tcpChannel);
        ControlMessage open = ControlMessage.open(host, port, requestId);
        sendText(ws, ControlMessageCodec.encode(open));
    }

    /**
     * opened 消息回填：按 requestId 找到本地 TCP 通道，建立双向映射（含承载该会话的 WS 通道），并 flush 缓冲数据。
     *
     * @param ws 收到该 opened 的 WS 通道（即承载此会话的通道）
     */
    public void onSessionOpened(Channel ws, long sessionId, String requestId) {
        Channel tcpChannel = pendingByRequestId.remove(requestId);
        if (tcpChannel == null) {
            log.warn("multiplexed Client 收到未知 requestId 的 opened: {}", requestId);
            return;
        }
        tcpChannelToSession.put(tcpChannel, sessionId);
        sessionToTcpChannel.put(sessionId, tcpChannel);
        sessionToWsChannel.put(sessionId, ws);
        log.info("multiplexed Client 会话映射建立: sessionId={} <-> tcp={} via ws={}",
                sessionId, tcpChannel.id().asShortText(), ws.id().asShortText());

        // flush 建立会话前缓存的数据。交给本地 TCP 通道的 eventLoop 执行，
        // 与后续 sendData 在同一条线程上序列化，保证顺序；key 在 flush 完成后才移除，
        // 使 flush 窗口内新到达的数据仍入同一队列而不乱序。数据帧统一走本会话绑定的 WS 通道。
        Queue<ByteBuf> q = pendingWrites.get(tcpChannel);
        if (q != null) {
            final Channel wsChannel = ws;
            final Queue<ByteBuf> queue = q;
            tcpChannel.eventLoop().execute(() -> {
                ByteBuf buf;
                while ((buf = queue.poll()) != null) {
                    if (wsChannel != null && wsChannel.isActive()) {
                        ByteBuf frame = DataFrameCodec.encodeZeroCopy(wsChannel.alloc(), sessionId, buf);
                        wsChannel.writeAndFlush(new BinaryWebSocketFrame(frame));
                    }
                    buf.release();
                }
                pendingWrites.remove(tcpChannel);
            });
        }
    }

    /**
     * 本地 TCP 收到数据时，打包成数据帧通过本会话绑定的 WS 通道发送给 server。
     * <p>
     * 会话尚未 opened（或仍在 flush 缓冲期间）时，先暂存数据，待 {@link #onSessionOpened} 回调后再统一发送，
     * 避免首包在 open→opened 往返完成前被静默丢弃（这正是之前不加 Thread.sleep 就卡死的根因）。
     */
    public void sendData(Channel tcpChannel, ByteBuf payload) {
        Long sessionId = tcpChannelToSession.get(tcpChannel);
        // 会话未就绪或仍在 flush 缓冲窗口内：入队等待，保持顺序
        if (sessionId == null || pendingWrites.containsKey(tcpChannel)) {
            Queue<ByteBuf> q = pendingWrites.computeIfAbsent(
                    tcpChannel, k -> new ConcurrentLinkedQueue<>());
            q.add(payload.retainedDuplicate());
            return;
        }
        Channel wsChannel = sessionToWsChannel.get(sessionId);
        if (wsChannel == null || !wsChannel.isActive()) {
            log.warn("multiplexed Client 无法发送数据：WS 断开 sessionId={}", sessionId);
            return;
        }
        ByteBuf frame = DataFrameCodec.encodeZeroCopy(wsChannel.alloc(), sessionId, payload);
        wsChannel.writeAndFlush(new BinaryWebSocketFrame(frame));
    }

    /**
     * 收到数据帧时，按 sessionId 写回对应本地 TCP 通道。
     */
    public void onData(long sessionId, ByteBuf payload) {
        Channel tcpChannel = sessionToTcpChannel.get(sessionId);
        if (tcpChannel != null && tcpChannel.isActive()) {
            tcpChannel.writeAndFlush(payload.retainedDuplicate());
        }
    }

    /**
     * 本地 TCP 断开时，清理会话映射并通知 server 关闭目标 TCP（走该会话绑定的 WS 通道）。
     */
    public void onTcpClosed(Channel tcpChannel) {
        Long sessionId = tcpChannelToSession.remove(tcpChannel);
        if (sessionId != null) {
            Channel ws = sessionToWsChannel.remove(sessionId);
            sessionToTcpChannel.remove(sessionId);
            log.info("multiplexed Client 本地 TCP 断开，清理会话映射 sessionId={}", sessionId);
            sendClose(ws, sessionId);
        }
        // 释放尚未 flush 的缓冲数据，避免内存泄漏
        Queue<ByteBuf> q = pendingWrites.remove(tcpChannel);
        if (q != null) {
            ByteBuf buf;
            while ((buf = q.poll()) != null) {
                buf.release();
            }
        }
    }

    /**
     * opened 失败：目标 TCP 连接不上。清理 pending 映射并释放缓冲，关闭本地 TCP，避免数据泄漏/挂起。
     */
    public void onSessionOpenFailed(String requestId) {
        Channel tcpChannel = pendingByRequestId.remove(requestId);
        if (tcpChannel == null) {
            return;
        }
        log.warn("multiplexed Client 会话建立失败，关闭本地 TCP requestId={}", requestId);
        Queue<ByteBuf> q = pendingWrites.remove(tcpChannel);
        if (q != null) {
            ByteBuf buf;
            while ((buf = q.poll()) != null) {
                buf.release();
            }
        }
        tcpChannel.close();
    }

    /**
     * 收到 server 的 close 消息时，关闭对应本地 TCP 连接。
     */
    public void onServerClose(long sessionId) {
        Channel tcpChannel = sessionToTcpChannel.remove(sessionId);
        sessionToWsChannel.remove(sessionId);
        if (tcpChannel != null) {
            tcpChannelToSession.remove(tcpChannel);
            log.info("multiplexed Client 收到 close，关闭本地 TCP sessionId={}", sessionId);
            tcpChannel.close();
        }
    }

    /**
     * 某条 WS 通道断开：从池中移除，并关闭落在该通道上的所有会话对应的本地 TCP，避免悬挂。
     */
    public void onWsClosed(Channel ws) {
        if (!wsPool.remove(ws)) {
            return;
        }
        log.warn("multiplexed Client WS 连接断开，清理其上的会话: {}", ws.id().asShortText());
        Iterator<Map.Entry<Long, Channel>> it = sessionToWsChannel.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Channel> e = it.next();
            if (e.getValue() == ws) {
                Long sessionId = e.getKey();
                Channel tcp = sessionToTcpChannel.remove(sessionId);
                if (tcp != null) {
                    tcpChannelToSession.remove(tcp);
                    if (tcp.isActive()) {
                        tcp.close();
                    }
                }
                it.remove();
            }
        }
        // 维持连接池容量：补充一条（带退避），不影响其它仍在工作的 WS 通道
        scheduleReconnect();
    }

    /**
     * 断线后自动重连，维持目标连接池大小。调度在独立后台线程，绝不占用 Netty event-loop 线程，
     * 因此 {@link #connectOne} 内的 connect().sync()/handshakeFuture().sync() 不会引发死锁。
     */
    private void scheduleReconnect() {
        if (closed || wsUri == null) {
            return;
        }
        if (wsPool.size() >= targetPoolSize) {
            reconnectAttempts.set(0);
            return;
        }
        int attempt = reconnectAttempts.incrementAndGet();
        long delay = Math.min(2000L, 200L * attempt); // 指数退避，上限 2s
        reconnectExecutor.schedule(() -> {
            if (closed) {
                return;
            }
            try {
                Channel ch = connectOne(wsUri);
                wsPool.add(ch);
                reconnectAttempts.set(0);
                log.info("multiplexed Client WS 重连成功，当前池大小={}", wsPool.size());
                if (wsPool.size() < targetPoolSize) {
                    scheduleReconnect(); // 继续补齐到目标数量
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("multiplexed Client WS 重连被中断");
            } catch (Exception e) {
                log.warn("multiplexed Client WS 重连失败（第{}次），{}ms 后重试: {}",
                        attempt, delay, e.getMessage());
                scheduleReconnect();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    public void awaitClose() {
        // 快照避免 onWsClosed 从池中移除导致的并发修改
        List<Channel> snapshot = new ArrayList<>(wsPool);
        try {
            for (Channel ch : snapshot) {
                ch.closeFuture().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        closed = true;
        reconnectExecutor.shutdownNow(); // 停止重连调度
        for (Channel ch : wsPool) {
            ch.close();
        }
        SharedEventLoopGroups.release(); // 共享 group，引用计数 -1，归零才真正关闭
    }
}
