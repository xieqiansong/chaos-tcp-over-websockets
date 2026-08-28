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
import lan.chaos.modules.tcp.over.websockets.v2.protocol.ControlMessage;
import lan.chaos.modules.tcp.over.websockets.v2.protocol.ControlMessageCodec;
import lan.chaos.modules.tcp.over.websockets.v2.util.SharedEventLoopGroups;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;

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

    public Client() {
        SharedEventLoopGroups.acquire(); // 共享 worker group，引用计数 +1
    }

    public void connect(String wsUrl) {
        log.info("v2 Client connect, wsUrl={}", wsUrl);
        try {
            URI uri = new URI(wsUrl);
            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), 8 * 1024 * 1024);
            WebSocketClientHandler handler = new WebSocketClientHandler(handshaker);

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
     * server 返回 opened（含 sessionId）后由 {@link WebSocketClientHandler} 处理。
     */
    public void openSession(String host, int port) {
        ControlMessage open = ControlMessage.open(host, port);
        sendText(ControlMessageCodec.encode(open));
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

    public static void main(String[] args) {
        String wsUrl = args.length > 0 ? args[0] : "ws://localhost:7002";
        Client client = new Client();
        client.connect(wsUrl);
        client.awaitClose();
    }
}
