package lan.chaos.modules.tcp.over.websockets.chunk;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * 按配置属性提供拆帧策略 Bean：
 * <ul>
 *   <li>{@code tcp.chunk.strategy}：{@code none}（不分帧，整块转发）或 {@code fixed}（按固定块切帧），默认 {@code fixed}。</li>
 *   <li>{@code tcp.chunk.size}：{@code fixed} 时的单块字节数，默认 {@code 1024}（1KB，流式且稳定）。</li>
 * </ul>
 */
@Slf4j
@Configuration
public class ChunkConfiguration {

    @Bean
    public ChunkStrategy chunkStrategy(Environment env) {
        String strategy = env.getProperty("tcp.chunk.strategy", "fixed").trim().toLowerCase();
        if ("none".equals(strategy)) {
            log.info("chunk 策略: no-slice（整块转发，不拆帧）");
            return new NoSliceChunkStrategy();
        }
        int size = Integer.parseInt(env.getProperty("tcp.chunk.size", "1024").trim());
        log.info("chunk 策略: fixed-slice-{}（按 {}B 拆帧）", size, size);
        return new FixedSliceChunkStrategy(size);
    }
}
