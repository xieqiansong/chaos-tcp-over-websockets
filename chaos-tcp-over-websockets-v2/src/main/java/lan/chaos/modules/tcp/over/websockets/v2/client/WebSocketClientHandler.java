package lan.chaos.modules.tcp.over.websockets.v2.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import lan.chaos.modules.tcp.over.websockets.v2.protocol.ControlMessage;
import lan.chaos.modules.tcp.over.websockets.v2.protocol.ControlMessageCodec;
import lan.chaos.modules.tcp.over.websockets.v2.protocol.DataFrameCodec;
import lombok.extern.slf4j.Slf4j;

/**
 * 负责 WebSocket 客户端握手，并处理控制消息与数据帧。
 */
@Slf4j
public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

    private final WebSocketClientHandshaker handshaker;
    private final Client client;
    private ChannelPromise handshakeFuture;

    public WebSocketClientHandler(WebSocketClientHandshaker handshaker, Client client) {
        this.handshaker = handshaker;
        this.client = client;
    }

    public ChannelPromise handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("v2 Client WebSocket 断开: {}", ctx.channel().id().asShortText());
        client.onWsClosed(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (!handshaker.isHandshakeComplete()) {
            try {
                handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                log.info("v2 Client WebSocket 握手成功: {}", ctx.channel().id().asShortText());
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                log.error("v2 Client WebSocket 握手失败: ", e);
                handshakeFuture.setFailure(e);
            }
            return;
        }

        if (msg instanceof FullHttpResponse) {
            throw new IllegalStateException("意外的 FullHttpResponse: " + ((FullHttpResponse) msg).status());
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof TextWebSocketFrame) {
            handleTextFrame(ctx, ((TextWebSocketFrame) frame).text());
        } else if (frame instanceof BinaryWebSocketFrame) {
            handleDataFrame((BinaryWebSocketFrame) frame);
        } else if (frame instanceof PingWebSocketFrame) {
            ctx.channel().writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
        } else if (frame instanceof CloseWebSocketFrame) {
            ctx.channel().close();
        }
    }

    private void handleTextFrame(ChannelHandlerContext ctx, String text) {
        ControlMessage msg = ControlMessageCodec.decode(text);
        if (msg == null) {
            log.info("v2 Client 收到普通文本: {}", text);
            return;
        }
        if ("opened".equals(msg.getType())) {
            client.onSessionOpened(ctx.channel(), msg.getSessionId(), msg.getRequestId());
        } else if ("openFailed".equals(msg.getType())) {
            client.onSessionOpenFailed(msg.getRequestId());
        } else if ("close".equals(msg.getType())) {
            client.onServerClose(msg.getSessionId());
        } else {
            log.warn("v2 Client 收到未知控制消息: {}", text);
        }
    }

    private void handleDataFrame(BinaryWebSocketFrame frame) {
        ByteBuf buf = frame.content();
        if (buf.readableBytes() < DataFrameCodec.HEADER_BYTES) {
            log.warn("v2 Client 收到过短数据帧，丢弃");
            return;
        }
        long sessionId = DataFrameCodec.decodeSessionId(buf);
        ByteBuf payload = DataFrameCodec.decodePayload(buf);
        client.onData(sessionId, payload);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("v2 Client 异常: ", cause);
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }
}
