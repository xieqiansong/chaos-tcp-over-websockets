package lan.chaos.modules.tcp.over.websockets.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lan.chaos.modules.tcp.over.websockets.server.WebsocketClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TcpServerHandler extends ChannelInboundHandlerAdapter {
    private final Map<String, WebsocketClient> websocketClientMap = new ConcurrentHashMap<>();
    private final String wsUrl;
    private final BufCopyStrategy bufCopyStrategy;

    public TcpServerHandler(String wsUrl, BufCopyStrategy bufCopyStrategy) {
        this.wsUrl = wsUrl;
        this.bufCopyStrategy = bufCopyStrategy;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        String channelId = ctx.channel().id().asLongText();
        if (log.isDebugEnabled()) {
            log.debug("tcp server active ....." + channelId);
            log.debug("tcp 客户端开始和 websocket 服务端建立连接, " + channelId);
        }
        // WebsocketClient 构造时已同步完成 connect + 握手，无需外部线程池执行 run() 保活
        WebsocketClient websocketClient = new WebsocketClient(wsUrl, ctx.channel(), bufCopyStrategy);
        websocketClientMap.put(channelId, websocketClient);
        log.debug("tcp 客户端开始和 websocket 服务端建立连接 结束");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        String channelId = ctx.channel().id().asLongText();
        if (log.isDebugEnabled()) {
            log.debug("客户端请求到了..." + channelId);
        }
        ByteBuf buf = (ByteBuf) msg;
        if (log.isDebugEnabled()) {
            log.debug("TCP server 收到的数据是:" + ByteBufUtil.hexDump(buf));
        }
        log.debug("开始转发tcp消息到websocket");
        WebsocketClient websocketClient = websocketClientMap.get(channelId);
        if (websocketClient != null) {
            log.debug("发送给 websocket client");
            // 零拷贝共享底层内存，引用计数 +1，写完成后由 Netty 自动 release
            ByteBuf buff = bufCopyStrategy.wrap(buf);
            websocketClient.writeAndFlush(buff);
        }
        // 入站 buf 处理完毕归还引用：copied 独立副本不受影响；retained 由 wrapped 持有引用，写完成自动释放
        ReferenceCountUtil.release(buf);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String channelId = ctx.channel().id().asLongText();
        WebsocketClient wc = websocketClientMap.remove(channelId);
        if (wc != null) {
            wc.close(); // 释放共享 group 引用计数
        }
        ctx.channel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("tcp server 遇到异常: " + cause);
        String channelId = ctx.channel().id().asLongText();
        WebsocketClient wc = websocketClientMap.remove(channelId);
        if (wc != null) {
            wc.close(); // 释放共享 group 引用计数
        }
        ctx.close();
    }
}