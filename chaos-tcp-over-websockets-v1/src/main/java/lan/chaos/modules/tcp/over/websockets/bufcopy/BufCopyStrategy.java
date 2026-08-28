package lan.chaos.modules.tcp.over.websockets.bufcopy;

import io.netty.buffer.ByteBuf;

/**
 * 隧道转发路径上的 ByteBuf 拷贝策略抽象。
 * 转发时需要对入站 ByteBuf 做"包装/拷贝"再交给对端 writeAndFlush，
 * 不同策略在吞吐与内存安全上权衡不同。
 */
public interface BufCopyStrategy {

    /**
     * 把 src 包装为可转发的 ByteBuf。返回的 buf 交给 Netty 的 writeAndFlush 即可，
     * 由其在写完成后负责 release。
     */
    ByteBuf wrap(ByteBuf src);

    /**
     * 该策略是否可在异步转发链路中安全使用。
     * duplicate 不持有引用计数，原 buf 释放后视图即悬空，链路中不安全。
     */
    boolean isSafeForForwarding();

    /** 策略名，用于日志与压测标注。 */
    String name();
}
