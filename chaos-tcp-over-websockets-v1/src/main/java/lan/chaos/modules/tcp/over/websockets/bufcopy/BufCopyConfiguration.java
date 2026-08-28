package lan.chaos.modules.tcp.over.websockets.bufcopy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * 按配置属性 {@code buf.copy.strategy}（copied | retained | duplicate）提供单例策略 Bean。
 * 默认 retained（零拷贝且链路安全）。
 * <p>
 * 注意：{@code duplicate} 在异步转发链路中不安全（不持有引用计数），此处显式拒绝，
 * 强制回退为 retained 并打印告警，避免误用导致数据悬空。
 */
@Slf4j
@Configuration
public class BufCopyConfiguration {

    @Bean
    public BufCopyStrategy bufCopyStrategy(Environment env) {
        String strategy = env.getProperty("buf.copy.strategy", "retained").trim().toLowerCase();
        switch (strategy) {
            case "copied":
                return new CopiedBufferStrategy();
            case "duplicate":
                log.warn("buf.copy.strategy=duplicate 在异步转发链路中不安全，已强制回退为 retained（仅 JMH 微基准可用）");
                return new RetainedDuplicateStrategy();
            case "retained":
            default:
                return new RetainedDuplicateStrategy();
        }
    }
}
