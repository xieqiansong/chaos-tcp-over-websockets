package lan.chaos.modules.tcp.over.websockets.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lan.chaos.modules.tcp.over.websockets.chunk.ChunkStrategy;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class WebsocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private final Channel tcpChannel;
    private final BufCopyStrategy bufCopyStrategy;
    private final ChunkStrategy chunkStrategy;
    private WebSocketClientHandshaker handshaker;
    private ChannelPromise channelPromise;


    public WebsocketClientHandler(Channel channel, BufCopyStrategy bufCopyStrategy, ChunkStrategy chunkStrategy) {
        this.tcpChannel = channel;
        this.bufCopyStrategy = bufCopyStrategy;
        this.chunkStrategy = chunkStrategy;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.channelPromise = ctx.newPromise();
    }

    public ChannelFuture handshakeFuture() {
        return this.channelPromise;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("成功建立 websocket 连接, 准备发送数据");
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        log.info("断开连接: " + ctx.channel().id().asLongText());
        tcpChannel.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        log.debug("收到消息, 判断 websocket 是否握手完毕: " + this.handshaker.isHandshakeComplete());
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            log.debug("websocket is bytebuf msg: " + ByteBufUtil.hexDump(buf));
        }
        Channel channel = ctx.channel();
        FullHttpResponse response;
        if (!this.handshaker.isHandshakeComplete()) {
            try {
                response = (FullHttpResponse) msg;
                this.handshaker.finishHandshake(ctx.channel(), response.retain());
                this.channelPromise.setSuccess();
                log.debug("websocket client handshark complete .....");
            } catch (WebSocketHandshakeException e) {
                log.error("websocket 握手失败, 错误信息: ", e);
                this.channelPromise.setFailure(e.getCause());
            }
        } else if (msg instanceof FullHttpResponse) {
            FullHttpResponse fullHttpResponse = (FullHttpResponse) msg;
            log.debug("websocket 握手响应");
            response = fullHttpResponse;
            log.error("Unexpetcd FullHttpResponse ( getStatus = " + response.status());
        } else {
            assert msg instanceof WebSocketFrame;
            WebSocketFrame frame = (WebSocketFrame) msg;
            log.debug("websocket server 响应数据: " + ByteBufUtil.hexDump(frame.content()));
            if (frame instanceof CloseWebSocketFrame) {
                log.debug("websocket 关闭请求");
                channel.close();
            } else if (frame instanceof TextWebSocketFrame) {
                log.debug("TextWebSocketFrame msg");
                this.tcpChannel.writeAndFlush(frame.content());
            } else if (frame instanceof BinaryWebSocketFrame) {
                log.debug("BinaryWebSocketFrame msg");
                // 按 chunk 策略拆帧转发；引用计数由策略内部保证
                chunkStrategy.transfer(frame.content(), bufCopyStrategy, this.tcpChannel::writeAndFlush);
            } else if (frame instanceof PingWebSocketFrame) {
                log.debug("心跳请求");
                ctx.channel().write(new PongWebSocketFrame(frame.content().retain()));
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("exceptionCaught: ", cause);
        ctx.close();
    }

}