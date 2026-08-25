package lan.chaos.modules.tcp.over.websockets.chunk;

import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

/** 按固定大小切成多块逐块转发，接近流式：块越小转发越平滑、延迟越低，但帧数越多。 */
public class FixedSliceChunkStrategy implements ChunkStrategy {

    private final int chunkSize;

    public FixedSliceChunkStrategy(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive, got " + chunkSize);
        }
        this.chunkSize = chunkSize;
    }

    @Override
    public void slice(ByteBuf src, Consumer<ByteBuf> sender) {
        while (src.isReadable()) {
            int len = Math.min(chunkSize, src.readableBytes());
            ByteBuf slice = src.readSlice(len);
            sender.accept(slice);
        }
    }

    @Override
    public String name() {
        return "fixed-slice-" + chunkSize;
    }
}
