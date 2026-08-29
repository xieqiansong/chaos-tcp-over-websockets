package lan.chaos.modules.tcp.over.websockets.v2.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * v2 Client 端本地 TCP 连接处理器。
 * <p>
 * 本地 TCP 连接进来后，向 server 发送 open 控制消息（携带目标地址）建立会话；
 * 收到本地 TCP 数据时打包成数据帧转发；连接断开时清理会话映射。
 */
@Slf4j
public class TcpServerHandler extends ChannelInboundHandlerAdapter {

    private final Client client;
    private final String targetHost;
    private final Integer targetPort;

    public TcpServerHandler(Client client, String targetHost, Integer targetPort) {
        this.client = client;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("v2 本地 TCP 连接进来: {}", ctx.channel().id().asShortText());
        // 目标地址先占位；实际应从配置/参数获取
        client.openSession(targetHost, targetPort, ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        client.sendData(ctx.channel(), buf);
        ReferenceCountUtil.release(buf);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("v2 本地 TCP 断开: {}", ctx.channel().id().asShortText());
        client.onTcpClosed(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("v2 本地 TCP 异常: ", cause);
        ctx.close();
    }
}
