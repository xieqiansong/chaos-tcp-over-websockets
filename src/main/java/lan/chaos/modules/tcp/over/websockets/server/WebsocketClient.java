package lan.chaos.modules.tcp.over.websockets.server;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import lan.chaos.modules.tcp.over.websockets.SharedEventLoopGroups;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
public class WebsocketClient {
    private final Channel channel;
    private final BufCopyStrategy bufCopyStrategy;

    public WebsocketClient(String wsUrl, final Channel tcpChannel, BufCopyStrategy bufCopyStrategy) {
        this.bufCopyStrategy = bufCopyStrategy;
        SharedEventLoopGroups.acquire(); // 共享 worker group，引用计数 +1
        log.debug("websocket client conn start. wsUrl:{}", wsUrl);
        Bootstrap bootstrap = new Bootstrap();
        Bootstrap ignored = SystemUtil.getOsInfo().isWindows() ? bootstrap.channel(NioSocketChannel.class) : bootstrap.channel(EpollSocketChannel.class);
        URI uri;
        try {
            uri = new URI(wsUrl);
        } catch (URISyntaxException e) {
            SharedEventLoopGroups.release();
            throw new RuntimeException(e);
        }
        // maxFramePayloadLength 默认 64KB，调大到 8MB 以支持大帧、减少大包被切成小帧的固定开销（与 server 端一致）
        final WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), 8 * 1024 * 1024);
        final WebsocketClientHandler wch = new WebsocketClientHandler(tcpChannel, bufCopyStrategy);
        bootstrap.group(SharedEventLoopGroups.worker())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        socketChannel.pipeline()
                                .addLast(new HttpClientCodec())
//                                .addLast(new LoggingHandler(LogLevel.INFO))
                                .addLast(new HttpObjectAggregator(8 * 1024 * 1024))
                                .addLast(wch);
                    }
                });
        try {
            this.channel = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
            handshaker.handshake(this.channel);
            wch.setHandshaker(handshaker);
            wch.handshakeFuture().sync();
            log.debug("websocket 握手成功");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeAndFlush(ByteBuf msg) {
        BinaryWebSocketFrame binaryWebSocketFrame = new BinaryWebSocketFrame(msg);
        this.channel.writeAndFlush(binaryWebSocketFrame).addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                log.debug("写入消息成功");
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("写入消息失败, " + channelFuture.cause().getMessage());
                }
            }
        });
    }

    public void close() {
        SharedEventLoopGroups.release(); // 共享 group，引用计数 -1，归零才真正关闭
        if (this.channel != null) {
            this.channel.close();
        }
    }

    // 原 run() 仅做 channel.closeFuture().sync() 阻塞驻留以保活连接引用。
    // 连接在构造时已同步建好，且被 websocketClientMap 强引用持有，不会 GC，
    // 故无需驻留线程；连接生命周期由 Netty EventLoop 管理。
}