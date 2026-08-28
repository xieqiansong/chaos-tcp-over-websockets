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

    public static ControlMessage open(String host, int port) {
        ControlMessage msg = new ControlMessage();
        msg.setType("open");
        msg.setHost(host);
        msg.setPort(port);
        return msg;
    }

    public static ControlMessage opened(long sessionId) {
        ControlMessage msg = new ControlMessage();
        msg.setType("opened");
        msg.setSessionId(sessionId);
        return msg;
    }
}
