package lan.chaos.modules.tcp.over.websockets.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lan.chaos.modules.tcp.over.websockets.chunk.ChunkStrategy;
import lan.chaos.modules.tcp.over.websockets.server.WebsocketClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class TcpServerHandler extends ChannelInboundHandlerAdapter {
    private final ExecutorService pool = Executors.newFixedThreadPool(32);
    private final Map<String, WebsocketClient> websocketClientMap = new ConcurrentHashMap<>();
    private final String wsUrl;
    private final BufCopyStrategy bufCopyStrategy;
    private final ChunkStrategy chunkStrategy;

    public TcpServerHandler(String wsUrl, BufCopyStrategy bufCopyStrategy, ChunkStrategy chunkStrategy) {
        this.wsUrl = wsUrl;
        this.bufCopyStrategy = bufCopyStrategy;
        this.chunkStrategy = chunkStrategy;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        String channelId = ctx.channel().id().asLongText();
        log.debug("tcp server active ....." + channelId);
        log.debug("tcp 客户端开始和 websocket 服务端建立连接, " + channelId);
        WebsocketClient websocketClient = new WebsocketClient(wsUrl, ctx.channel(), bufCopyStrategy, chunkStrategy);
        websocketClientMap.put(channelId, websocketClient);
        pool.execute(websocketClient);
        log.debug("tcp 客户端开始和 websocket 服务端建立连接 结束");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        String channelId = ctx.channel().id().asLongText();
        log.debug("客户端请求到了..." + channelId);
        ByteBuf buf = (ByteBuf) msg;
        log.debug("TCP server 收到的数据是:" + ByteBufUtil.hexDump(buf));
        log.debug("开始转发tcp消息到websocket");
        WebsocketClient websocketClient = websocketClientMap.get(channelId);
        if (websocketClient != null) {
            log.debug("发送给 websocket client");
            // 按 chunk 策略拆帧转发（可配置：整块 / 固定块）；引用计数由策略内部保证
            chunkStrategy.transfer(buf, bufCopyStrategy, websocketClient::writeAndFlush);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String channelId = ctx.channel().id().asLongText();
        websocketClientMap.remove(channelId);
        ctx.channel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("tcp server 遇到异常: " + cause);
        ctx.close();
    }
}