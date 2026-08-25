package lan.chaos.modules.tcp.over.websockets.benchmark;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lan.chaos.modules.tcp.over.websockets.bufcopy.BufCopyStrategy;
import lan.chaos.modules.tcp.over.websockets.bufcopy.CopiedBufferStrategy;
import lan.chaos.modules.tcp.over.websockets.bufcopy.DuplicateStrategy;
import lan.chaos.modules.tcp.over.websockets.bufcopy.RetainedDuplicateStrategy;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * 隧道转发路径上 ByteBuf 拷贝策略的「轻量对照测试」——可在 IDEA 中直接右键 run，
 * 无需命令行。与 {@link BufCopyStrategyBenchmark}(JMH 正式基准) 互补：
 * 本测试仅用于快速观察不同策略在不同消息尺寸下的吞吐量级差异，不做 JVM 预热、
 * 不受 GC 干扰控制，数据仅供定性参考。
 *
 * <p>直接复用刚落地的 {@code bufcopy} 包三实现，按 {@code isSafeForForwarding()}
 * 决定是否在写完端 release——duplicate 共享引用计数，故不在循环内 release，避免提前
 * 释放源 buf（这正是它链路不安全的体现）。
 */
public class BufCopyStrategyComparisonTest {

    private static final List<Integer> SIZES = Arrays.asList(1024, 65536, 1_048_576);
    private static final int ITERATIONS = 200_000;

    @Test
    void compareThroughput() {
        List<BufCopyStrategy> strategies = Arrays.asList(
                new CopiedBufferStrategy(),
                new RetainedDuplicateStrategy(),
                new DuplicateStrategy()
        );

        for (int size : SIZES) {
            System.out.println("==== size=" + size + " bytes, iterations=" + ITERATIONS + " ====");
            for (BufCopyStrategy strategy : strategies) {
                ByteBuf src = Unpooled.buffer(size);
                src.writerIndex(size);
                src.setByte(size - 1, 0xAA);

                long start = System.nanoTime();
                for (int i = 0; i < ITERATIONS; i++) {
                    ByteBuf out = strategy.wrap(src);
                    // 安全策略各自持有引用计数，写完端 release 以贴合真实转发生命周期
                    if (strategy.isSafeForForwarding()) {
                        out.release();
                    }
                }
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                double opsPerMs = ITERATIONS * 1.0 / Math.max(elapsedMs, 1);
                System.out.printf("  %-9s %10.2f ops/ms  (%d ms)%n", strategy.name(), opsPerMs, elapsedMs);

                src.release();
            }
        }
    }
}
