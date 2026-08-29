package lan.chaos.modules.tcp.over.websockets.v2.client;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import lan.chaos.modules.tcp.over.websockets.v2.protocol.ControlMessage;
import lan.chaos.modules.tcp.over.websockets.v2.protocol.ControlMessageCodec;
import lan.chaos.modules.tcp.over.websockets.v2.protocol.DataFrameCodec;
import lan.chaos.modules.tcp.over.websockets.v2.util.SharedEventLoopGroups;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * v2 Client（骨架）。
 * <p>
 * 建立到 server 的 WebSocket 连接并完成握手（握手逻辑见 {@link WebSocketClientHandler}）。
 * 提供 {@link #openSession} 向 server 发送会话建立控制消息。
 * 使用共享 EventLoopGroup（{@link SharedEventLoopGroups}），后续步骤再补充会话管理、多会话复用等。
 */
@Slf4j
public class Client {

    private Channel channel;

    /** 本地 TCP 通道 -> sessionId（open 后由 opened 消息回填）。 */
    private final Map<Channel, Long> tcpChannelToSession = new ConcurrentHashMap<>();

    /** sessionId -> 本地 TCP 通道（收到数据帧时按 sessionId 写回）。 */
    private final Map<Long, Channel> sessionToTcpChannel = new ConcurrentHashMap<>();

    /** requestId -> 本地 TCP 通道（open 发出后等待 opened 回填）。 */
    private final Map<String, Channel> pendingByRequestId = new ConcurrentHashMap<>();

    /**
     * 本地 TCP 会话尚未 opened 前，暂存本地 TCP 收到的数据 payload。
     * key 存在即表示"仍在等待/flush 缓冲"，后续到达的数据继续入队以保证有序。
     */
    private final Map<Channel, Queue<ByteBuf>> pendingWrites = new ConcurrentHashMap<>();

    private final AtomicLong requestIdSeq = new AtomicLong(0);

    public Client() {
        SharedEventLoopGroups.acquire(); // 共享 worker group，引用计数 +1
    }

    public void connect(String wsUrl) {
        log.info("v2 Client connect, wsUrl={}", wsUrl);
        try {
            URI uri = new URI(wsUrl);
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

            channel = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
            handler.handshakeFuture().sync();
            log.info("v2 Client 已建立连接: {}", channel.id().asShortText());
        } catch (URISyntaxException e) {
            log.error("v2 Client 非法 wsUrl: {}", wsUrl, e);
            shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("v2 Client 连接被中断: ", e);
            shutdown();
        }
    }

    public void sendText(String text) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(new TextWebSocketFrame(text));
            log.info("v2 Client 发送文本: {}", text);
        }
    }

    /**
     * 向 server 发送 open 控制消息，请求建立会话。
     * server 返回 opened（含 sessionId + requestId）后由 {@link WebSocketClientHandler} 回填映射。
     *
     * @param tcpChannel 本地 TCP 通道，用于建立 sessionId 映射
     */
    public void openSession(String host, int port, Channel tcpChannel) {
        String requestId = String.valueOf(requestIdSeq.incrementAndGet());
        pendingByRequestId.put(requestId, tcpChannel);
        ControlMessage open = ControlMessage.open(host, port, requestId);
        sendText(ControlMessageCodec.encode(open));
    }

    /**
     * opened 消息回填：按 requestId 找到本地 TCP 通道，建立双向映射，并 flush 缓冲的数据。
     */
    public void onSessionOpened(long sessionId, String requestId) {
        Channel tcpChannel = pendingByRequestId.remove(requestId);
        if (tcpChannel == null) {
            log.warn("v2 Client 收到未知 requestId 的 opened: {}", requestId);
            return;
        }
        tcpChannelToSession.put(tcpChannel, sessionId);
        sessionToTcpChannel.put(sessionId, tcpChannel);
        log.info("v2 Client 会话映射建立: sessionId={} <-> tcp={}", sessionId, tcpChannel.id().asShortText());

        // flush 建立会话前缓存的数据。交给本地 TCP 通道的 eventLoop 执行，
        // 与后续 sendData 在同一条线程上序列化，保证顺序；key 在 flush 完成后才移除，
        // 使 flush 窗口内新到达的数据仍入同一队列而不乱序。
        Queue<ByteBuf> q = pendingWrites.get(tcpChannel);
        if (q != null) {
            final Channel wsChannel = channel;
            final Queue<ByteBuf> queue = q;
            tcpChannel.eventLoop().execute(() -> {
                ByteBuf buf;
                while ((buf = queue.poll()) != null) {
                    if (wsChannel != null && wsChannel.isActive()) {
                        ByteBuf frame = DataFrameCodec.encode(sessionId, buf);
                        wsChannel.writeAndFlush(new BinaryWebSocketFrame(frame));
                    }
                    buf.release();
                }
                pendingWrites.remove(tcpChannel);
            });
        }
    }

    /**
     * 本地 TCP 收到数据时，打包成数据帧发送给 server。
     * <p>
     * 会话尚未 opened（或仍在 flush 缓冲期间）时，先暂存数据，待 {@link #onSessionOpened} 回调后再统一发送，
     * 避免首包在 open→opened 往返完成前被静默丢弃（这正是之前不加 Thread.sleep 就卡死的根因）。
     */
    public void sendData(Channel tcpChannel, ByteBuf payload) {
        if (channel == null || !channel.isActive()) {
            log.warn("v2 Client 无法发送数据：WS 断开");
            return;
        }
        Long sessionId = tcpChannelToSession.get(tcpChannel);
        // 会话未就绪或仍在 flush 缓冲窗口内：入队等待，保持顺序
        if (sessionId == null || pendingWrites.containsKey(tcpChannel)) {
            Queue<ByteBuf> q = pendingWrites.computeIfAbsent(
                    tcpChannel, k -> new ConcurrentLinkedQueue<>());
            q.add(payload.retainedDuplicate());
            return;
        }
        ByteBuf frame = DataFrameCodec.encode(sessionId, payload);
        channel.writeAndFlush(new BinaryWebSocketFrame(frame));
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
     * 本地 TCP 断开时，清理会话映射并通知 server 关闭目标 TCP。
     */
    public void onTcpClosed(Channel tcpChannel) {
        Long sessionId = tcpChannelToSession.remove(tcpChannel);
        if (sessionId != null) {
            sessionToTcpChannel.remove(sessionId);
            log.info("v2 Client 本地 TCP 断开，清理会话映射 sessionId={}", sessionId);
            ControlMessage close = ControlMessage.close(sessionId);
            sendText(ControlMessageCodec.encode(close));
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
     * 收到 server 的 close 消息时，关闭对应本地 TCP 连接。
     */
    public void onServerClose(long sessionId) {
        Channel tcpChannel = sessionToTcpChannel.remove(sessionId);
        if (tcpChannel != null) {
            tcpChannelToSession.remove(tcpChannel);
            log.info("v2 Client 收到 close，关闭本地 TCP sessionId={}", sessionId);
            tcpChannel.close();
        }
    }

    public void awaitClose() {
        if (channel == null) {
            return;
        }
        try {
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        SharedEventLoopGroups.release(); // 共享 group，引用计数 -1，归零才真正关闭
    }

//    public static void main(String[] args) {
//        String wsUrl = args.length > 0 ? args[0] : "ws://localhost:7002";
//        Client client = new Client();
//        client.connect(wsUrl);
//        client.awaitClose();
//    }
}
