package lan.chaos.modules.tcp.over.websockets.chunk;

import io.netty.buffer.ByteBuf;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;

/**
 * 按固定大小切成多块逐块转发，接近流式：块越小转发越平滑、延迟越低，但帧数越多。
 * 使用 readRetainedSlice 使每个切片持有 src 的引用计数，保证异步写出期间底层内存存活。
 */
public class FixedSliceChunkStrategy implements ChunkStrategy {

    private final int chunkSize;

    public FixedSliceChunkStrategy(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive, got " + chunkSize);
        }
        this.chunkSize = chunkSize;
    }

    @Override
    public void transfer(ByteBuf src, BufCopyStrategy copy, Sink sink) {
        while (src.isReadable()) {
            int len = Math.min(chunkSize, src.readableBytes());
            ByteBuf slice = src.readRetainedSlice(len);   // slice 持有 src 引用，refCnt 独立
            ByteBuf wrapped = copy.wrap(slice);           // retained: slice.retainedDuplicate() → slice refCnt+1
            slice.release();                              // 释放 readRetainedSlice 的 +1 → slice 引用归 wrapped 持有
            sink.write(wrapped);
        }
    }

    @Override
    public String name() {
        return "fixed-slice-" + chunkSize;
    }
}
