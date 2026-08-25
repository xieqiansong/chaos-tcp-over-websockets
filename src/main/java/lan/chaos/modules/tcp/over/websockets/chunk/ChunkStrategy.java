package lan.chaos.modules.tcp.over.websockets.chunk;

import io.netty.buffer.ByteBuf;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;

/**
 * 隧道转发路径上的"拆帧"策略抽象。
 * 决定入站 ByteBuf 是整块一次转发（no-slice），还是按固定大小切成多块逐块转发（fixed-slice）。
 * 拆帧粒度影响单 WS 帧大小与转发延迟：块越小越接近流式、越平滑，但帧数越多。
 * <p>
 * 引用计数责任：{@code transfer} 内部完成切片与 {@code wrap}，并把写出的 buf 交给 {@code sink}；
 * 切片是否 release、如何与 wrap 配合保证异步写出期间底层存活，由各实现自己负责。
 */
public interface ChunkStrategy {

    /** 写出目标：把已 wrap 好的 buf 交给下游 writeAndFlush。 */
    interface Sink {
        void write(ByteBuf buf);
    }

    /** 把 src 按本策略切分、wrap，并把每个结果交给 sink 写出。实现必须保证引用计数正确。 */
    void transfer(ByteBuf src, BufCopyStrategy copy, Sink sink);

    /** 策略名，用于日志与压测标注。 */
    String name();
}
