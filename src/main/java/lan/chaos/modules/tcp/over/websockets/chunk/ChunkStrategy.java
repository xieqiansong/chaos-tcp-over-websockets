package lan.chaos.modules.tcp.over.websockets.chunk;

import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

/**
 * 隧道转发路径上的"拆帧"策略抽象。
 * 决定入站 ByteBuf 是整块一次转发（no-slice），还是按固定大小切成多块逐块转发（fixed-slice）。
 * 拆帧粒度影响单 WS 帧大小与转发延迟：块越小越接近流式、越平滑，但帧数越多。
 * <p>
 * 引用计数责任：{@code slice} 只负责切分并把每个切片交给 {@code sender}；
 * 由 sender（handler 层）对切片做 {@code bufCopyStrategy.wrap} 并 writeAndFlush，
 * 因此本接口不持有也不释放任何 ByteBuf 引用。
 */
public interface ChunkStrategy {

    /**
     * 把 src 按本策略切分，每个切片通过 sender 交给下游（sender 内负责 wrap 与写出）。
     * 实现必须用 readSlice 之类不额外持有引用的切法，生命周期由 src 与下游 wrap 共同保证。
     */
    void slice(ByteBuf src, Consumer<ByteBuf> sender);

    /** 策略名，用于日志与压测标注。 */
    String name();
}
