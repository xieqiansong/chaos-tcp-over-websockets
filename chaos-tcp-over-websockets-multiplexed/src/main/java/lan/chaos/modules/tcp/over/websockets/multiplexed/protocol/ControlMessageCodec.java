package lan.chaos.modules.tcp.over.websockets.multiplexed.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

/**
 * 控制消息 JSON 编解码。
 */
@Slf4j
public final class ControlMessageCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ControlMessageCodec() {
    }

    public static String encode(ControlMessage msg) {
        try {
            return MAPPER.writeValueAsString(msg);
        } catch (Exception e) {
            throw new RuntimeException("控制消息序列化失败", e);
        }
    }

    public static ControlMessage decode(String json) {
        try {
            return MAPPER.readValue(json, ControlMessage.class);
        } catch (Exception e) {
            log.warn("控制消息解析失败: {}", json, e);
            return null;
        }
    }
}
