package lan.chaos.modules.tcp.over.websockets.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import lan.chaos.modules.tcp.over.websockets.utils.OsInfo;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
public class WebsocketClient implements Runnable {
    private final Channel channel;

    public WebsocketClient(String wsUrl, final Channel tcpChannel) {
        log.debug("websocket client conn start. wsUrl:{}", wsUrl);
        EventLoopGroup workGroup = OsInfo.isWindows ? new NioEventLoopGroup() : new EpollEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        Bootstrap ignored = OsInfo.isWindows ? bootstrap.channel(NioSocketChannel.class) : bootstrap.channel(EpollSocketChannel.class);
        URI uri;
        try {
            uri = new URI(wsUrl);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        final WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());
        final WebsocketClientHandler wch = new WebsocketClientHandler(tcpChannel);
        bootstrap.group(workGroup)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        socketChannel.pipeline()
                                .addLast(new HttpClientCodec())
//                                .addLast(new LoggingHandler(LogLevel.INFO))
                                .addLast(new HttpObjectAggregator(65536))
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
                log.debug("写入消息失败, " + channelFuture.cause().getMessage());
            }
        });
    }

    @Override
    public void run() {
        try {
            this.channel.closeFuture().sync();
        } catch (Exception e) {
            log.error("websocket client 建立连接失败, 错误信息: ", e);
        }
        log.info("websocket client over");
    }
}