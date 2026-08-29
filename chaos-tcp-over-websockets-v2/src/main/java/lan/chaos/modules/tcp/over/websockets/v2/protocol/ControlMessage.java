package lan.chaos.modules.tcp.over.websockets.v2.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * 控制面消息（文本 JSON）。
 * <p>
 * 会话建立阶段用文本帧承载 JSON 控制消息，后续数据流转为二进制帧。
 * 消息类型由 {@code type} 字段区分。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ControlMessage {

    /** 消息类型。 */
    private String type;

    /** 目标主机（open 时由 client 携带）。 */
    private String host;

    /** 目标端口（open 时由 client 携带）。 */
    private Integer port;

    /** 会话 ID（opened 时由 server 返回）。 */
    private Long sessionId;

    /** 请求关联 ID（open 时 client 生成，opened 时 server 原样回带，用于匹配）。 */
    private String requestId;

    public static ControlMessage open(String host, int port, String requestId) {
        ControlMessage msg = new ControlMessage();
        msg.setType("open");
        msg.setHost(host);
        msg.setPort(port);
        msg.setRequestId(requestId);
        return msg;
    }

    public static ControlMessage opened(long sessionId, String requestId) {
        ControlMessage msg = new ControlMessage();
        msg.setType("opened");
        msg.setSessionId(sessionId);
        msg.setRequestId(requestId);
        return msg;
    }

    public static ControlMessage openFailed(long sessionId, String requestId) {
        ControlMessage msg = new ControlMessage();
        msg.setType("openFailed");
        msg.setSessionId(sessionId);
        msg.setRequestId(requestId);
        return msg;
    }

    public static ControlMessage close(long sessionId) {
        ControlMessage msg = new ControlMessage();
        msg.setType("close");
        msg.setSessionId(sessionId);
        return msg;
    }
}
