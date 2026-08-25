package lan.chaos.modules.tcp.over.websockets.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lan.chaos.modules.tcp.over.websockets.client.TcpClient;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.netty.handler.codec.http.HttpUtil.isKeepAlive;
import static io.netty.handler.codec.http.HttpUtil.setContentLength;

@Slf4j
public class WebsocketServerHandler extends SimpleChannelInboundHandler<Object> {
    private final ExecutorService pool = Executors.newFixedThreadPool(32);
    private final Map<String, TcpClient> tcpClientMap = new ConcurrentHashMap<>();
    private final BufCopyStrategy bufCopyStrategy;
    private WebSocketServerHandshaker handshaker;

    public WebsocketServerHandler(BufCopyStrategy bufCopyStrategy) {
        this.bufCopyStrategy = bufCopyStrategy;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        log.debug("成功建立连接...");
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        String channelId = ctx.channel().id().asLongText();
        log.debug("websocket 断开连接: " + channelId);
        TcpClient tcpClient = tcpClientMap.remove(channelId);
        if (tcpClient != null && !tcpClient.isClose()) {
            tcpClient.close();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpMessage) {
            log.debug("收到 FullHttpMessage");
            handleHttpMsg(ctx, (FullHttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            WebSocketFrame webSocketFrame = (WebSocketFrame) msg;
            log.debug("收到简单的客户端消息 channel id: " + ctx.channel().id().asLongText());
            handlerWebSocketFrame(ctx, webSocketFrame);
        }
    }

    private void handlerWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame webSocketFrame) {
        log.debug("websocket 收到消息：" + webSocketFrame.toString());
        if (webSocketFrame instanceof CloseWebSocketFrame) {
            log.debug("关闭请求");
            handshaker.close(ctx.channel(), ((CloseWebSocketFrame) webSocketFrame).retain());
            return;
        }
        if (webSocketFrame instanceof PingWebSocketFrame) {
            log.debug("心跳请求");
            ctx.channel().write(new PongWebSocketFrame(webSocketFrame.content().retain()));
            return;
        }
        if (webSocketFrame instanceof TextWebSocketFrame) {
            // 字符串类型消息处理
            String responseMsg = ((TextWebSocketFrame) webSocketFrame).text();
            log.debug("文本消息 " + responseMsg);
            String channelId = ctx.channel().id().asLongText();
            TcpClient tcpClient = tcpClientMap.get(channelId);
            if (tcpClient != null) {
                ByteBuf buff = Unpooled.copiedBuffer(responseMsg, StandardCharsets.UTF_8);
                tcpClient.writeAndFlush(buff);
            }
        }
        if (webSocketFrame instanceof BinaryWebSocketFrame) {
            log.debug("收到 二进制消息, 开始转发");
            String channelId = ctx.channel().id().asLongText();
            TcpClient tcpClient = tcpClientMap.get(channelId);
            if (tcpClient != null) {
                // 零拷贝共享帧内容底层内存，引用计数 +1，写完成后由 Netty 自动 release
                ByteBuf buff = bufCopyStrategy.wrap(((BinaryWebSocketFrame) webSocketFrame).content());
                tcpClient.writeAndFlush(buff);
            }
        }
    }

    private void handleHttpMsg(ChannelHandlerContext ctx, FullHttpRequest req) {
        //如果http解码失败，返回http异常
        //判断是否是WebSocket握手请求
        if (!req.decoderResult().isSuccess()
                || (!"websocket".equals(req.headers().get("Upgrade")))) {
            sentHttpResponse(ctx, req, new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST));
            return;
        }
        String uri = req.uri();

        // 目标地址 目标端口
        String[] paths = uri.split("/", Integer.MIN_VALUE);
        String operation = paths[1];
        if ("forward".equals(operation)) {
            String targetHost = paths[2];
            Integer targetPort = Integer.parseInt(paths[3]);

            log.info("开始建立 tcp 连接开始 targetHost: {} targetPort {}", targetHost, targetPort);
            TcpClient tcpClient = new TcpClient(targetHost, targetPort, ctx.channel(), bufCopyStrategy);
            tcpClientMap.put(ctx.channel().id().asLongText(), tcpClient);
            pool.execute(tcpClient);
            log.info("开始建立 tcp 连接 结束");

            //构造握手响应返回
            WebSocketServerHandshakerFactory wsFactory =
                    new WebSocketServerHandshakerFactory("", null, false);
            handshaker = wsFactory.newHandshaker(req);
            if (handshaker == null) {
                WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
            } else {
                handshaker.handshake(ctx.channel(), req);
            }
        } else {
            throw new RuntimeException("unknown operation");
        }
    }

    private void sentHttpResponse(ChannelHandlerContext ctx, FullHttpRequest req, DefaultFullHttpResponse res) {
        if (res.status().code() != 200) {
            ByteBuf byteBuf = Unpooled.copiedBuffer(res.status().toString(), CharsetUtil.UTF_8);
            res.content().writeBytes(byteBuf);
            byteBuf.release();
            setContentLength(res, res.content().readableBytes());
        }

        //如果是非keep-alive连接，关闭连接
        ChannelFuture future = ctx.channel().writeAndFlush(res);
        if (!isKeepAlive(req) || res.status().code() != 200) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.debug("exceptionCaught: ", cause);
        ctx.close();
    }

}