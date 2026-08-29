package lan.chaos.modules.tcp.over.websockets.multiplexed.server;

import cn.hutool.system.SystemUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.ControlMessage;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.ControlMessageCodec;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.DataFrameCodec;
import lan.chaos.modules.tcp.over.websockets.multiplexed.session.Session;
import lan.chaos.modules.tcp.over.websockets.multiplexed.session.SessionManager;
import lan.chaos.modules.tcp.over.websockets.multiplexed.util.SharedEventLoopGroups;
import lombok.extern.slf4j.Slf4j;

/**
 * 负责 WebSocket 升级握手，并处理会话控制消息与数据帧。
 * <p>
 * 收到 open 控制消息后：分配 sessionId、连接目标 TCP，并通过 opened 消息返回。
 * 收到数据帧（二进制）后：按 sessionId 路由到对应 Session 的目标 TCP。
 */
@Slf4j
public class WebSocketUpgradeHandler extends SimpleChannelInboundHandler<Object> {

    private final SessionManager sessionManager;
    private WebSocketServerHandshaker handshaker;

    public WebSocketUpgradeHandler(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest) {
            handleHttpRequest(ctx, (FullHttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
        if (!req.decoderResult().isSuccess()
                || !"websocket".equalsIgnoreCase(req.headers().get(HttpHeaderNames.UPGRADE))) {
            DefaultFullHttpResponse res =
                    new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
            sendHttpResponse(ctx, res);
            return;
        }

        WebSocketServerHandshakerFactory factory =
                new WebSocketServerHandshakerFactory(getWebSocketLocation(req), null, false, 8 * 1024 * 1024);
        handshaker = factory.newHandshaker(req);
        if (handshaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
        } else {
            handshaker.handshake(ctx.channel(), req);
            log.info("multiplexed Server WebSocket 握手成功: {}", ctx.channel().id().asShortText());
        }
    }

    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (frame instanceof CloseWebSocketFrame) {
            handshaker.close(ctx.channel(), ((CloseWebSocketFrame) frame).retain());
            return;
        }
        if (frame instanceof PingWebSocketFrame) {
            ctx.channel().writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
            return;
        }
        if (frame instanceof TextWebSocketFrame) {
            handleControlMessage(ctx, ((TextWebSocketFrame) frame).text());
            return;
        }
        if (frame instanceof BinaryWebSocketFrame) {
            handleDataFrame(ctx, (BinaryWebSocketFrame) frame);
        }
    }

    private void handleControlMessage(ChannelHandlerContext ctx, String text) {
        ControlMessage msg = ControlMessageCodec.decode(text);
        if (msg == null) {
            log.warn("multiplexed Server 收到无法解析的控制消息: {}", text);
            return;
        }
        if ("open".equals(msg.getType())) {
            Session session = sessionManager.create(msg.getHost(), msg.getPort(), ctx.channel());
            // 异步连接目标 TCP：不能在 event-loop 线程里 sync，否则命中同线程时会抛 BlockingOperationException
            connectTarget(session, msg.getHost(), msg.getPort(), msg.getRequestId(), ctx);
            log.info("multiplexed Server 收到 open，开始异步连接目标 {}:{}, sessionId={}",
                    msg.getHost(), msg.getPort(), session.getSessionId());
        } else if ("close".equals(msg.getType())) {
            Session session = sessionManager.remove(msg.getSessionId());
            if (session != null) {
                session.close();
                log.info("multiplexed Server 收到 close，关闭会话 sessionId={}", msg.getSessionId());
            }
        } else {
            log.warn("multiplexed Server 收到未知控制消息类型: {}", msg.getType());
        }
    }

    private void handleDataFrame(ChannelHandlerContext ctx, BinaryWebSocketFrame frame) {
        ByteBuf buf = frame.content();
        if (buf.readableBytes() < DataFrameCodec.HEADER_BYTES) {
            log.warn("multiplexed Server 收到过短数据帧，丢弃");
            return;
        }
        long sessionId = DataFrameCodec.decodeSessionId(buf);
        Session session = sessionManager.get(sessionId);
        if (session == null || session.getTargetTcpChannel() == null) {
            log.warn("multiplexed Server 收到未知 sessionId={} 的数据帧，丢弃", sessionId);
            return;
        }
        ByteBuf payload = DataFrameCodec.decodePayload(buf);
        // 复制一份，避免在异步写完成后引用被释放的 WS 帧内容
        session.getTargetTcpChannel().writeAndFlush(payload.retainedDuplicate());
    }

    private void connectTarget(Session session, String host, int port, String requestId, ChannelHandlerContext wsCtx) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(SharedEventLoopGroups.worker())
                .channel(SystemUtil.getOsInfo().isWindows() ? NioSocketChannel.class : EpollSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new TargetTcpHandler(session.getSessionId(), session.getWsChannel(), sessionManager));
                    }
                });
        ChannelFuture cf = bootstrap.connect(host, port);
        cf.addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                Channel targetChannel = future.channel();
                // 切回 WS 通道所在 eventLoop 更新会话并回 opened，避免跨线程竞争 targetTcpChannel
                wsCtx.channel().eventLoop().execute(() -> {
                    session.setTargetTcpChannel(targetChannel);
                    ControlMessage opened = ControlMessage.opened(session.getSessionId(), requestId);
                    wsCtx.channel().writeAndFlush(new TextWebSocketFrame(ControlMessageCodec.encode(opened)));
                    log.info("multiplexed Server 已连接目标 TCP {}:{}, sessionId={}", host, port, session.getSessionId());
                });
            } else {
                log.error("multiplexed Server 连接目标 TCP 失败 {}:{}: ", host, port, future.cause());
                sessionManager.remove(session.getSessionId());
                wsCtx.channel().eventLoop().execute(() -> {
                    ControlMessage failed = ControlMessage.openFailed(session.getSessionId(), requestId);
                    wsCtx.channel().writeAndFlush(new TextWebSocketFrame(ControlMessageCodec.encode(failed)));
                });
            }
        });
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        log.info("multiplexed Server WebSocket 断开: {}", ctx.channel().id().asShortText());
        sessionManager.removeByWsChannel(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("multiplexed Server 异常: ", cause);
        ctx.close();
    }

    private static String getWebSocketLocation(FullHttpRequest req) {
        return "ws://" + req.headers().get(HttpHeaderNames.HOST) + req.uri();
    }

    private static void sendHttpResponse(ChannelHandlerContext ctx, DefaultFullHttpResponse res) {
        if (res.status().code() != 200) {
            res.content().writeBytes(Unpooled.copiedBuffer(res.status().toString(), CharsetUtil.UTF_8));
        }
        ChannelFuture f = ctx.channel().writeAndFlush(res);
        if (res.status().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
