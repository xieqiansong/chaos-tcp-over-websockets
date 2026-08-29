package lan.chaos.modules.tcp.over.websockets.multiplexed.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.ControlMessage;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.ControlMessageCodec;
import lan.chaos.modules.tcp.over.websockets.multiplexed.protocol.DataFrameCodec;
import lan.chaos.modules.tcp.over.websockets.multiplexed.session.SessionManager;
import lombok.extern.slf4j.Slf4j;

/**
 * Server 端目标 TCP 连接处理器：目标 TCP 返回的数据，打包成数据帧发回 client。
 * 目标 TCP 断开时，通过 close 控制消息通知 client 关闭本地 TCP。
 */
@Slf4j
public class TargetTcpHandler extends ChannelInboundHandlerAdapter {

    private final long sessionId;
    private final Channel wsChannel;
    private final SessionManager sessionManager;

    public TargetTcpHandler(long sessionId, Channel wsChannel, SessionManager sessionManager) {
        this.sessionId = sessionId;
        this.wsChannel = wsChannel;
        this.sessionManager = sessionManager;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        if (wsChannel != null && wsChannel.isActive()) {
            // 零拷贝：payload 逻辑拼接进帧（帧内部已 retain），写完后由帧级联释放
            ByteBuf frame = DataFrameCodec.encodeZeroCopy(wsChannel.alloc(), sessionId, buf);
            wsChannel.writeAndFlush(new BinaryWebSocketFrame(frame));
        }
        ReferenceCountUtil.release(buf);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("multiplexed 目标 TCP 断开, sessionId={}", sessionId);
        sessionManager.remove(sessionId);
        if (wsChannel != null && wsChannel.isActive()) {
            ControlMessage close = ControlMessage.close(sessionId);
            wsChannel.writeAndFlush(new TextWebSocketFrame(ControlMessageCodec.encode(close)));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("multiplexed 目标 TCP 异常, sessionId={}: ", sessionId, cause);
        ctx.close();
    }
}
