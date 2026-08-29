package lan.chaos.modules.tcp.over.websockets.multiplexed.session;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 会话管理器（server 端）：分配 sessionId、登记/查询/清理会话。
 * <p>
 * 当前步只负责会话握手阶段的登记；数据转发与按连接批量清理留待后续。
 */
@Slf4j
public class SessionManager {

    private final AtomicLong idSequence = new AtomicLong(0);
    private final Map<Long, Session> bySessionId = new ConcurrentHashMap<>();

    /**
     * 创建并登记一个新会话，分配自增 sessionId。
     */
    public Session create(String targetHost, int targetPort, Channel wsChannel) {
        long sessionId = idSequence.incrementAndGet();
        Session session = new Session(sessionId, targetHost, targetPort, wsChannel);
        bySessionId.put(sessionId, session);
        log.info("会话已创建: {}", session);
        return session;
    }

    public Session get(long sessionId) {
        return bySessionId.get(sessionId);
    }

    public Session remove(long sessionId) {
        Session session = bySessionId.remove(sessionId);
        if (session != null) {
            log.info("会话已移除: {}", session);
        }
        return session;
    }

    /**
     * 清理某条 WS 连接上的所有会话（连接断开时调用），并关闭对应目标 TCP。
     */
    public void removeByWsChannel(Channel wsChannel) {
        bySessionId.entrySet().removeIf(entry -> {
            if (entry.getValue().getWsChannel() == wsChannel) {
                log.info("连接断开，移除会话: {}", entry.getValue());
                entry.getValue().close();
                return true;
            }
            return false;
        });
    }
}
