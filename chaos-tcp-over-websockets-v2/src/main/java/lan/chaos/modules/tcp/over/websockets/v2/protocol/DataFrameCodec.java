package lan.chaos.modules.tcp.over.websockets.v2.protocol;

import io.netty.buffer.ByteBuf;
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
