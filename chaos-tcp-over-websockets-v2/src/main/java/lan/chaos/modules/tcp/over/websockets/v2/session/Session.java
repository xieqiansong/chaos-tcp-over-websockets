package lan.chaos.modules.tcp.over.websockets.v2.session;

import io.netty.channel.Channel;
import lombok.Data;

/**
 * 会话（骨架）。
 * <p>
 * 一个 Session 对应一条「本地 TCP ↔ 目标 TCP」的逻辑隧道。
 * 本步仅建立会话握手：client 发 open → server 分配 sessionId 返回。
 * 后续步骤再挂接本端 TCP Channel 与数据转发。
 */
@Data
public class Session {

    /** 会话 ID，由 server 端生成。 */
    private final long sessionId;

    /** 目标主机（open 时携带）。 */
    private final String targetHost;

    /** 目标端口。 */
    private final int targetPort;

    /** 承载该会话的 WebSocket 通道。 */
    private final Channel wsChannel;

    /** 本端 TCP 通道（后续数据流转时挂接，当前为 null）。 */
    private volatile Channel tcpChannel;

    public Session(long sessionId, String targetHost, int targetPort, Channel wsChannel) {
        this.sessionId = sessionId;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.wsChannel = wsChannel;
    }

    @Override
    public String toString() {
        return "Session{id=" + sessionId + ", target=" + targetHost + ":" + targetPort
                + ", ws=" + wsChannel.id().asShortText() + "}";
    }
}
