package lan.chaos.modules.tcp.over.websockets.benchmark;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * 隧道转发路径上 ByteBuf 拷贝策略的 JMH 微基准（放在 test 源码，不参与 main 打包）。
 *
 * 转发逻辑中每次转发都调用 {@code Unpooled.copiedBuffer(src)} 做一次全量拷贝
 * （见 TcpServerHandler / TcpClientHandler / WebsocketServerHandler / WebsocketClientHandler）。
 * 本基准衡量「全量拷贝」与两种「零拷贝」策略(duplicate / retainedDuplicate)在
 * 不同消息尺寸下的吞吐差异，为是否改为零拷贝提供依据。
 *
 * 运行方式（手动）:
 *   mvn test-compile exec:java -Dexec.mainClass=org.openjdk.jmh.Main \
 *       -Dexec.classpathScope=test -Dexec.args="BufCopyStrategyBenchmark -f 0"
 * 说明: exec 环境下 JMH 子进程 fork 依赖自身 classpath，故以 -f 0 进程内运行；
 *       正式性能数据建议在独立 fork 环境执行完整参数。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
@Threads(1)
public class BufCopyStrategyBenchmark {

    @Param({"1024", "65536", "1048576"})
    private int size;

    private ByteBuf src;

    @Setup
    public void setup() {
        src = Unpooled.buffer(size);
        // 填充部分字节，避免全零内存的复制优化掩盖真实拷贝开销
        src.writerIndex(size);
        src.setByte(size - 1, 0xAA);
    }

    @TearDown
    public void tearDown() {
        src.release();
    }

    /**
     * 当前实现：全量拷贝为新的独立 ByteBuf，分配新内存并复制全部字节。
     */
    @Benchmark
    public ByteBuf copiedBuffer(Blackhole bh) {
        ByteBuf out = Unpooled.copiedBuffer(src);
        bh.consume(out);
        out.release();
        return out;
    }

    /**
     * 零拷贝 v1：共享底层内存，仅建立新的索引视图，不增加引用计数，无分配开销。
     */
    @Benchmark
    public ByteBuf duplicate(Blackhole bh) {
        ByteBuf out = src.duplicate();
        bh.consume(out);
        return out;
    }

    /**
     * 零拷贝 v2：共享底层内存并递增引用计数，配合写端 release 保证生命周期安全。
     */
    @Benchmark
    public ByteBuf retainedDuplicate(Blackhole bh) {
        ByteBuf out = src.retainedDuplicate();
        bh.consume(out);
        out.release();
        return out;
    }
}