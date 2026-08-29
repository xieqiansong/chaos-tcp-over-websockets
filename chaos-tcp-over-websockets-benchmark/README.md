# chaos-tcp-over-websockets-benchmark

`chaos-tcp-over-websockets` 的**性能基准模块**：`src/main` 放 JMH 微基准，`src/test` 放端到端隧道压测，**不参与业务发布**（simple 的主发布 jar 不包含本模块）。

包含两套基准：

| 基准 | 位置 | 测什么 |
|---|---|---|
| [`BufCopyStrategyBenchmark`](src/main/java/lan/chaos/modules/tcp/over/websockets/benchmark/BufCopyStrategyBenchmark.java) | `src/main` | JMH 微基准：三种 `ByteBuf` 拷贝策略的吞吐对拍 |
| [`TunnelRealWorldBenchmarkTest`](src/test/java/lan/chaos/modules/tcp/over/websockets/benchmark/TunnelRealWorldBenchmarkTest.java) | `src/test` | 端到端压测：simple / multiplexed 两条隧道 + 直连 echo 的吞吐与 RTT 对比 |

## 一、JMH 微基准（BufCopyStrategyBenchmark）

### 测什么

隧道转发路径上每次转发都涉及 ByteBuf 处理，本基准对比三种策略在不同消息尺寸（1KB / 64KB / 1MB）下的吞吐（单线程、Throughput、ops/s）：

- `copiedBuffer`：`Unpooled.copiedBuffer(src)` 全量拷贝，返回独立副本（链路安全）。
- `duplicate`：共享底层内存，仅建立新索引视图，**不递增引用计数**。
- `retainedDuplicate`：共享底层内存并递增 refCnt，配合写端 `release()` 保证生命周期安全。

### 运行

```bash
# 方式一（推荐）：打包 uber jar，JMH fork 出干净 JVM
mvn -pl chaos-tcp-over-websockets-benchmark -am package -DskipTests
java -jar chaos-tcp-over-websockets-benchmark/target/benchmarks.jar BufCopyStrategyBenchmark

# 方式二：免打包，进程内快速验证（-f 0 不 fork）
mvn -pl chaos-tcp-over-websockets-benchmark -am compile exec:java \
    "-Dexec.mainClass=org.openjdk.jmh.Main" "-Dexec.args=BufCopyStrategyBenchmark -f 0"
```

### 结果要点

| 策略 | 1KB | 64KB | 1MB |
|---|---|---|---|
| `copiedBuffer`（全量拷贝） | ~9.7M | ~134K | ~8.5K |
| `duplicate`（零拷贝视图，不计数） | ~200M | ~197M | ~199M |
| `retainedDuplicate`（零拷贝 + 引用计数） | ~51M | ~51M | ~51M |

- 全量拷贝吞吐随消息尺寸几乎线性下降（1KB→1MB 掉约 1000 倍），零拷贝系列与尺寸无关。
- `duplicate` 数字含 JVM 逃逸分析假象且**不持有引用计数**，真实链路中不安全，不宜采用；
- `retainedDuplicate` 数字可信且不随尺寸下降，是零拷贝迁移的推荐策略。

## 二、端到端隧道压测（TunnelRealWorldBenchmarkTest）

### 拓扑与参数

- 数据源：普通 `Socket` 并发打流（默认 4 连接，每连接 16MB，合计 64MB）。
- 被测对象：
  - `tunnel_simple`：simple 隧道 client 入口（本地端口 21001）
  - `tunnel_multiplexed`：multiplexed 隧道 client 入口（本地端口 21002）
  - `direct`：直连 echo 后端（端口 20001，无隧道，作为对照基线）
- 指标：吞吐（MB/s，端到端 received 字节 / 耗时）、RTT（ms，单轮「发出→完整收齐回包」平均时延）。
- 关键设计：**单次单组合运行**（一个包大小 + 一个直连对照），避免多组合混跑的状态污染。

### 运行

> 前置：测试只负责灌流统计，**simple / multiplexed 隧道服务与 echo 后端需先启动**（服务分别监听 21001 / 21002 / 20001）。

```bash
# 在仓库根目录，先安装上游模块
mvn -pl chaos-tcp-over-websockets-benchmark -am install -DskipTests -Dcheckstyle.skip=true

# 逐档单跑（每档一次 JVM，包大小 {1KB,4KB,16KB,64KB,256KB,1MB}）
for P in 1024 4096 16384 65536 262144 1048576; do
  mvn -pl chaos-tcp-over-websockets-benchmark test \
    -Dtest=TunnelRealWorldBenchmarkTest#runSingle \
    -Dbench.payload=$P -Dcheckstyle.skip=true -Dmaven.test.failure.ignore=true
done
```

结果以标准 CSV 追加至 `chaos-tcp-over-websockets-benchmark/target/bench-results.log`：

`timestamp, strategy, payloadBytes, tunnelMbPerSec, tunnelRttMs, tunnelReceived, tunnelError, directMbPerSec, directRttMs, directReceived, directError`

### 结果要点

| 包大小 | tunnel_simple | tunnel_multiplexed | direct |
|---|---:|---:|---:|
| 1KB | 10.67 MB/s | 13.34 MB/s | 57.61 MB/s |
| 64KB | 157.25 MB/s | 400.00 MB/s | 1777.78 MB/s |
| 1MB | 245.21 MB/s | 592.59 MB/s | 888.89 MB/s |

- multiplexed 同包大小下吞吐约为 simple 的 1.3～2.5 倍，RTT 更低。
- 1MB 档 multiplexed（592 MB/s）已接近直连（888 MB/s）。

> 详细结论与瓶颈分析见 [`chaos-tcp-over-websockets-multiplexed/TEST_REPORT.md`](../chaos-tcp-over-websockets-multiplexed/TEST_REPORT.md) 与 [`chaos-tcp-over-websockets-simple/TEST_REPORT.md`](../chaos-tcp-over-websockets-simple/TEST_REPORT.md)（simple 的基准报告为早期主代码状态，记录了「每包固定开销主导」的瓶颈模型）。

## 三、EchoServer（辅助工具）

纯网络通讯用的 TCP echo 服务端（收到什么原样回什么，不含隧道逻辑），用于脱离隧道单独验证 TCP / Netty 网络通讯本身：

```bash
mvn -pl chaos-tcp-over-websockets-benchmark -am compile exec:java \
    "-Dexec.mainClass=lan.chaos.modules.tcp.over.websockets.benchmark.EchoServer" \
    "-Dexec.args=20001"
```

不传端口时默认监听 `20001`。

## License

[MIT](../LICENSE)（参考来源项目保留其原始版权与协议）。
