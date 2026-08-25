package lan.chaos.modules.tcp.over.websockets.chunk;

import io.netty.buffer.ByteBuf;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;

/** 不分帧：整块一次转发（等价于改造前的原始行为）。 */
public class NoSliceChunkStrategy implements ChunkStrategy {

    @Override
    public void transfer(ByteBuf src, BufCopyStrategy copy, Sink sink) {
        if (src.isReadable()) {
            // wrap 持有 src 引用（retainedDuplicate）或独立拷贝（copied），
            // src 在 channelRead 返回后由 Netty release，但 wrapped 仍可安全写出。
            sink.write(copy.wrap(src));
        }
    }

    @Override
    public String name() {
        return "no-slice";
    }
}
