package lan.chaos.modules.tcp.over.websockets.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lan.chaos.modules.tcp.over.websockets.chunk.ChunkStrategy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TcpClientHandler extends ChannelInboundHandlerAdapter {
    private final Channel websocketChannel;
    private final BufCopyStrategy bufCopyStrategy;
    private final ChunkStrategy chunkStrategy;

    public TcpClientHandler(Channel channel, BufCopyStrategy bufCopyStrategy, ChunkStrategy chunkStrategy) {
        this.websocketChannel = channel;
        this.bufCopyStrategy = bufCopyStrategy;
        this.chunkStrategy = chunkStrategy;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.debug("tcp client channel active " + ctx.channel().id().asLongText());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.debug("TCP client 收到服务器响应..." + ctx.channel().id().asLongText());
        ByteBuf buf = (ByteBuf) msg;
        log.debug("TCP client 服务端响应的数据是:" + ByteBufUtil.hexDump(buf));
        if (websocketChannel != null) {
            // 按 chunk 策略拆帧转发；引用计数由策略内部保证
            chunkStrategy.transfer(buf, bufCopyStrategy, wrapped -> {
                log.debug("tcp client 开始发送数据 " + ByteBufUtil.hexDump(wrapped));
                websocketChannel.writeAndFlush(new BinaryWebSocketFrame(wrapped));
            });
        }
        // 入站 buf 归还引用；wrapped 独立持有或共享引用，写完成自动释放
        ReferenceCountUtil.release(buf);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("TCP client 断开连接:" + ctx.channel().id().asLongText());
        ctx.channel().close();
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("tcp server 遇到异常: " + cause);
        ctx.channel().close();
        ctx.close();
    }
}