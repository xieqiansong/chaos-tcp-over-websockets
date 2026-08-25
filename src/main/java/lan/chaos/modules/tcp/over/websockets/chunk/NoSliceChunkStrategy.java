package lan.chaos.modules.tcp.over.websockets.chunk;

import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

/** 不分帧：整块一次转发（等价于改造前的原始行为）。 */
public class NoSliceChunkStrategy implements ChunkStrategy {

    @Override
    public void slice(ByteBuf src, Consumer<ByteBuf> sender) {
        if (src.isReadable()) {
            sender.accept(src);
        }
    }

    @Override
    public String name() {
        return "no-slice";
    }
}
