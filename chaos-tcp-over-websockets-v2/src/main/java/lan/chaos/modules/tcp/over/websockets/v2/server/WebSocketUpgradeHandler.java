package lan.chaos.modules.tcp.over.websockets.v2.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
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
import lan.chaos.modules.tcp.over.websockets.v2.protocol.ControlMessage;
import lan.chaos.modules.tcp.over.websockets.v2.protocol.ControlMessageCodec;
import lan.chaos.modules.tcp.over.websockets.v2.session.Session;
import lan.chaos.modules.tcp.over.websockets.v2.session.SessionManager;
import lombok.extern.slf4j.Slf4j;

/**
 * 负责 WebSocket 升级握手，并处理会话控制消息。
 * <p>
 * 收到 open 控制消息后，分配 sessionId 并通过 opened 消息返回给 client。
 * 数据流转为二进制帧（后续步骤处理）。
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
        // 非 WebSocket 升级请求，直接返回 400
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
            log.info("v2 Server WebSocket 握手成功: {}", ctx.channel().id().asShortText());
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
            String text = ((TextWebSocketFrame) frame).text();
            handleControlMessage(ctx, text);
            return;
        }
        if (frame instanceof BinaryWebSocketFrame) {
            // 数据流转发留待后续步骤
            log.debug("v2 Server 收到二进制帧, {} bytes，暂不处理", frame.content().readableBytes());
        }
    }

    private void handleControlMessage(ChannelHandlerContext ctx, String text) {
        ControlMessage msg = ControlMessageCodec.decode(text);
        if (msg == null) {
            log.warn("v2 Server 收到无法解析的控制消息: {}", text);
            return;
        }
        if ("open".equals(msg.getType())) {
            Session session = sessionManager.create(msg.getHost(), msg.getPort(), ctx.channel());
            ControlMessage opened = ControlMessage.opened(session.getSessionId());
            ctx.channel().writeAndFlush(new TextWebSocketFrame(ControlMessageCodec.encode(opened)));
            log.info("v2 Server 已分配 sessionId={} 给目标 {}:{}", session.getSessionId(), msg.getHost(), msg.getPort());
        } else {
            log.warn("v2 Server 收到未知控制消息类型: {}", msg.getType());
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        log.info("v2 Server WebSocket 断开: {}", ctx.channel().id().asShortText());
        sessionManager.removeByWsChannel(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("v2 Server 异常: ", cause);
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
