package lan.chaos.modules.tcp.over.websockets.v2.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 * v2 Client 端本地 TCP 连接处理器（骨架）。
 * <p>
 * 本地 TCP 连接进来后，向 server 发送 open 控制消息（携带目标地址），
 * 建立会话握手。数据转发留待后续步骤。
 */
@Slf4j
public class TcpServerHandler extends ChannelInboundHandlerAdapter {

    private final Client client;

    public TcpServerHandler(Client client) {
        this.client = client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("v2 本地 TCP 连接进来: {}", ctx.channel().id().asShortText());
        // 骨架：目标地址先用占位；实际应从配置/参数获取
        client.openSession("127.0.0.1", 30100);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // 数据转发留待后续步骤
        log.debug("v2 本地 TCP 收到数据，暂不转发");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("v2 本地 TCP 断开: {}", ctx.channel().id().asShortText());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("v2 本地 TCP 异常: ", cause);
        ctx.close();
    }
}
