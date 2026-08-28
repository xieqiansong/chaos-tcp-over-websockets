package lan.chaos.modules.tcp.over.websockets.v2.session;

import io.netty.channel.Channel;
import lombok.Data;

/**
 * 会话：一条「本地 TCP ↔ 目标 TCP」的逻辑隧道。
 * <p>
 * Server 端视角：{@link #wsChannel} 承载该会话的 WS 连接，{@link #targetTcpChannel}
 * 是连到目标服务的 TCP 连接。数据经 sessionId 路由。
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

    /** 目标 TCP 通道（open 后连接）。 */
    private volatile Channel targetTcpChannel;

    public Session(long sessionId, String targetHost, int targetPort, Channel wsChannel) {
        this.sessionId = sessionId;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.wsChannel = wsChannel;
    }

    public void close() {
        if (targetTcpChannel != null) {
            targetTcpChannel.close();
        }
    }

    @Override
    public String toString() {
        return "Session{id=" + sessionId + ", target=" + targetHost + ":" + targetPort
                + ", ws=" + wsChannel.id().asShortText() + "}";
    }
}
