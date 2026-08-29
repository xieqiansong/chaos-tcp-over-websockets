package lan.chaos.modules.tcp.over.websockets.simple.bufcopy;

import io.netty.buffer.ByteBuf;

/**
 * 零拷贝（不增引用）：仅建立共享底层内存的索引视图，无分配、无引用计数。
 * 注意：原 buf 一旦被 release，本视图即悬空，异步转发链路中不安全，
 * 仅用于 JMH 微基准对照观察，不可用于真实转发。
 */
public class DuplicateStrategy implements BufCopyStrategy {
    @Override
    public ByteBuf wrap(ByteBuf src) {
        return src.duplicate();
    }

    @Override
    public boolean isSafeForForwarding() {
        return false;
    }

    @Override
    public String name() {
        return "duplicate";
    }
}
