package lan.chaos.modules.tcp.over.websockets.bufcopy;

import io.netty.buffer.ByteBuf;

/** 零拷贝：共享底层内存并递增引用计数（CAS），写完成后由 Netty 自动 release，生命周期安全。 */
public class RetainedDuplicateStrategy implements BufCopyStrategy {
    @Override
    public ByteBuf wrap(ByteBuf src) {
        return src.retainedDuplicate();
    }

    @Override
    public boolean isSafeForForwarding() {
        return true;
    }

    @Override
    public String name() {
        return "retained";
    }
}
