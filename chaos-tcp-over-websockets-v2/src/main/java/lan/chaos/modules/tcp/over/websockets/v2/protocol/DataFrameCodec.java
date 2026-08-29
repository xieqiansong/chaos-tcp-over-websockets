package lan.chaos.modules.tcp.over.websockets.v2.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

/**
 * 数据帧编解码：数据流转发用二进制帧。
 * <pre>
 * +------------+----------+------------------+
 * | sessionId  |  length  |     payload      |
 * | (8 bytes)  | (4 bytes)|  (length bytes)  |
 * +------------+----------+------------------+
 * </pre>
 * sessionId 为 long（大端），length 为 int（大端，payload 字节数）。
 * length 字段用于在流式 TCP/WS 上界定每个完整 payload 的边界（拆包/粘包）。
 */
public final class DataFrameCodec {

    /** sessionId 头长度（long = 8 字节）。 */
    public static final int SESSION_ID_BYTES = 8;

    /** length 头长度（int = 4 字节）。 */
    public static final int LENGTH_BYTES = 4;

    /** 帧头总长度。 */
    public static final int HEADER_BYTES = SESSION_ID_BYTES + LENGTH_BYTES;

    private DataFrameCodec() {
    }

    /**
     * 将 payload 包装为带 sessionId + length 头的数据帧。
     * 返回的 ByteBuf 由调用方负责 release。
     */
    public static ByteBuf encode(long sessionId, ByteBuf payload) {
        int len = payload.readableBytes();
        ByteBuf buf = Unpooled.buffer(HEADER_BYTES + len);
        buf.writeLong(sessionId);
        buf.writeInt(len);
        buf.writeBytes(payload, payload.readerIndex(), len);
        return buf;
    }

    /**
     * 零拷贝版编码：头部由 alloc 分配，payload 通过 CompositeByteBuf 逻辑拼接，不复制数据。
     * 方法内部会对 payload 做一次 retain，返回的帧持有该引用；调用方可继续自行管理
     * （释放）原 payload 引用。帧释放时会级联释放 header 与这份 payload 引用。
     */
    public static ByteBuf encodeZeroCopy(ByteBufAllocator alloc, long sessionId, ByteBuf payload) {
        ByteBuf header = alloc.buffer(HEADER_BYTES);
        header.writeLong(sessionId);
        header.writeInt(payload.readableBytes());
        return Unpooled.wrappedBuffer(header, payload.retain());
    }

    /**
     * 从数据帧中解析出 sessionId。
     */
    public static long decodeSessionId(ByteBuf buf) {
        return buf.getLong(buf.readerIndex());
    }

    /**
     * 从数据帧中解析出 payload 长度。
     */
    public static int decodeLength(ByteBuf buf) {
        return buf.getInt(buf.readerIndex() + SESSION_ID_BYTES);
    }

    /**
     * 跳过帧头，返回 payload 部分（共享底层内存，不复制）。
     */
    public static ByteBuf decodePayload(ByteBuf buf) {
        return buf.slice(buf.readerIndex() + HEADER_BYTES,
                buf.readableBytes() - HEADER_BYTES);
    }
}
