package lan.chaos.modules.tcp.over.websockets.bufcopy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/** 全量拷贝：分配新内存并复制全部字节，返回独立副本。最安全但随消息尺寸线性变慢。 */
public class CopiedBufferStrategy implements BufCopyStrategy {
    @Override
    public ByteBuf wrap(ByteBuf src) {
        return Unpooled.copiedBuffer(src);
    }

    @Override
    public boolean isSafeForForwarding() {
        return true;
    }

    @Override
    public String name() {
        return "copied";
    }
}
