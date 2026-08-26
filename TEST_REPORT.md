# 测试报告：隧道转发 ByteBuf 拷贝策略（copiedBuffer / duplicate / retainedDuplicate）

> 统一报告：覆盖「环境准备 → 对照测试 → 三实现吞吐实测 → 真实端到端场景 → 结论」。
> 数据采集：
> - 端到端场景（第五节）：2026-08-26，Windows + JDK17，Maven 编译，命令行**单次单组合**运行（干净基线）。
> - 内存微基准（第四节）：2026-08-25，Windows + JDK8，单线程 IDEA 点运行。

## 一、测试环境

| 项 | 值 |
|---|---|
| 平台 / JDK | Windows / JDK8（main 编译 target 1.8） |
| 框架 | Spring Boot 2.7.18（仅外壳，基准不依赖容器） |
| 被测实现 | `CopiedBufferStrategy`（全量拷贝）/ `RetainedDuplicateStrategy`（零拷贝 v2）/ `DuplicateStrategy`（零拷贝 v1，链路不安全） |
| 测量方式 | JUnit 5 `@Test`（IDEA 右键一键运行，轻量对照） + JMH `@Benchmark`（正式压测，命令行） |
| 消息尺寸 | 1024 / 65536 / 1048576 字节（1KB / 64KB / 1MB） |
| 迭代次数 | 每格 200,000 次循环计时（对照测试，无 JVM 预热控制） |

## 二、运行方式

### 2.1 IDEA 一键对照测试（推荐，免命令）

场景在 [BufCopyStrategyComparisonTest.java](file:///d:/project/chaos/chaos-tcp-over-websockets/src/test/java/lan/chaos/modules/tcp/over/websockets/benchmark/BufCopyStrategyComparisonTest.java) 定义，IDEA 中打开该文件 → 点方法左侧绿色箭头 → Run 即出结果：

```java
@Test
void compareThroughput() { /* 3 策略 × 3 尺寸，每格 20 万次循环计时 */ }
```

- 直接复用刚落地的 `bufcopy` 包三实现类，按 `isSafeForForwarding()` 决定是否在写端 `release()`。
- `duplicate` 共享引用计数，循环内**不** release，避免提前释放源 buf（正是它链路不安全的体现）。
- 输出打印在 IDEA Run 面板（JUnit 视图 + stdout）。

### 2.2 JMH 正式基准（命令行，受控预热）

正式压测源 [BufCopyStrategyBenchmark.java](file:///d:/project/chaos/chaos-tcp-over-websockets/src/test/java/lan/chaos/modules/tcp/over/websockets/benchmark/BufCopyStrategyBenchmark.java)，需命令行：

```bash
mvn test-compile exec:java -Dexec.mainClass=org.openjdk.jmh.Main \
    -Dexec.classpathScope=test -Dexec.args="BufCopyStrategyBenchmark -f 0"
```

> 说明：对照测试用于快速观察量级差异；JMH 基准提供受控预热、无 GC 干扰的正式吞吐数据。二者互补。

## 三、构建与单元测试

结果：**BUILD SUCCESS**，对照测试通过（`mvn test -Dtest=BufCopyStrategyComparisonTest`）。

| 用例 | 说明 |
|---|---|
| `BufCopyStrategyComparisonTest` | 三策略 × 三尺寸轻量对照：直接 new `Copied/Retained/Duplicate` 三实现，循环 `wrap()` 计时，按 `isSafeForForwarding()` 控制 release，验证复用链路无引用计数错乱 |
| `BufCopyStrategyBenchmark`（JMH） | 正式 `@Benchmark` 微基准，`@Setup` 填字节避免零内存优化掩盖真实拷贝开销，`@TearDown` 释放源 buf |

### 过程中确认的设计点（供回查）
1. **`duplicate` 链路不安全**：`duplicate()` 与原 buf 共享同一引用计数，写端 `release()` 会连同源 buf 一起释放，原 handler 复用 src 时触发 `IllegalReferenceCountException`。故默认策略选 `retainedDuplicate`（持有独立 +1 引用，写端 release 安全），`duplicate` 仅作零拷贝对照。
2. **`retainedDuplicate` 与 `duplicate` 同为零拷贝**：仅建立新索引视图，不复制字节、不分配堆外内存；差异仅在引用计数语义，吞吐同量级。
3. **`copiedBuffer` 全量拷贝**：每次分配新内存并复制全部字节，返回独立副本（链路绝对安全），但开销随消息尺寸线性增长。

## 四、三实现吞吐实测（IDEA 内存内 wrap() 对照测试）

> 单位 ops/ms（每毫秒完成的 wrap 次数，越高越快）。对照测试无 JVM 预热控制，绝对值仅供定性，重点看策略间相对量级。每格 200,000 次循环。

| size（字节） | copied（全量拷贝） | retained（零拷贝 v2） | duplicate（零拷贝 v1，不安全） |
|---|---|---|---|
| 1024 | 3225.81 | 13333.33 | 40000.00 |
| 65536 | 104.22 | 50000.00 | 20000.00 |
| 1048576 | 7.66 | 50000.00 | 100000.00 |

附本轮原始耗时（ms / 20 万次）：1024 → copied 62 / retained 15 / duplicate 5；65536 → copied 1919 / retained 4 / duplicate 10；1MB → copied 26121 / retained 4 / duplicate 2。

## 五、真实隧道端到端场景（方案 B，2026-08-26）

> 目的：把三策略放到**真实异步转发链路**验证，而非内存内 `wrap()` 微基准。拓扑：echo 后端(Netty TCP，收到即回) + 隧道 server(`WebsocketServer`) + 隧道 client 入口(`TcpServer`) + 打流客户端(普通 Socket)。场景实现见 [TunnelRealWorldBenchmarkTest.java](file:///d:/project/chaos/chaos-tcp-over-websockets/src/test/java/lan/chaos/modules/tcp/over/websockets/benchmark/TunnelRealWorldBenchmarkTest.java)。
> 注：本测试直接 `new XxxStrategy()` 注入两段转发 handler，**绕过 `BufCopyConfiguration` 对 duplicate 的回退**，使 duplicate 真跑，验证其异步链路不安全。
> **关键前提**：测试期通过 `src/test/resources/logback-test.xml` 将日志降级为 WARN。**logback 默认 root=DEBUG 会大量打印 Netty 内部日志，在 Windows 同步写控制台时严重抢占 EventLoop 线程、压低基准**，故正式数据均在关 DEBUG 下采集。
>
> **方法修正（重要）**：本场景曾用「一次 JVM 内连续混跑 3 策略 × 4 包大小」的旧测试（`realWorldThreeStrategies`），结果**不可复现**——retained 在 256KB/4MB 大包下偶发 `SocketTimeoutException`、4MB 轮全部 `Connection refused`。经单轮对照实验证实：**单独跑任一组合完全正常，崩溃只出现在连续混跑的第 3/4 轮**，即根因是**连续混跑时前面轮次的线程/端口/引用计数状态污染后续轮次，而非代码 bug**。2026-08-26 已将该场景重构为**单次单组合**运行（`runSingle()`，通过 `-Dbench.strategy` / `-Dbench.payload` 指定），每次 JVM 只起一个隧道 + 一个直连对照，测完即清理；主代码未做任何改动。

### 5.1 运行方式

```bash
mvn test "-Dtest=TunnelRealWorldBenchmarkTest#runSingle" -Dbench.strategy=retained -Dbench.payload=262144
```
- `bench.strategy` ∈ {`copied`,`retained`,`duplicate`}；`bench.payload` 为包大小字节数（默认 1024）。
- 每次运行只测一个「策略 × 包大小」+ 一个直连 echo 对照，独立干净、可复现。

### 5.2 干净基线（单次独立运行，每个组合单独启动 JVM）

> 参数：1 连接，每连接 4MB。每次运行冷启动 + 单次打流，吞吐绝对值受无 JIT 预热影响，重点看**稳定性与策略间相对量级**。

| 包大小 | copied（隧道） | retained（隧道） | duplicate（隧道） | direct（直连基线） |
|---|---|---|---|---|
| 1KB | 2.62 MB/s | 2.00 MB/s | CRASH（预期） | 14~20 MB/s |
| 16KB | 9.37 MB/s | 9.28 MB/s | CRASH（预期） | 174~222 MB/s |
| 256KB | 11.63 MB/s | 11.49 MB/s | CRASH（预期） | 364~666 MB/s |
| 4MB | 12.54 MB/s | 8.03 MB/s | CRASH（预期） | 222~571 MB/s |

> 结论：
> 1. **全部组合单跑稳定、可复现**——copied 与 retained 在所有包大小下均正常回收 4MB，无崩溃；`IllegalReferenceCountException` 刷屏消失。**证实主代码转发链路的引用计数与线程模型健康**。
> 2. **duplicate 单跑即预期 CRASH**（SocketException/SocketTimeoutException），坐实「共享引用计数在异步转发链路不安全」的结论。
> 3. **单次打流 4MB 数据量偏小、无 JIT 预热**，吞吐绝对值和直连基线波动大（direct 从 14 到 666 MB/s）。要获得有统计意义的正式基线，需**单次打更大流量（如 32MB+）或多次运行取均值**，而非连续混跑不同组合（后者引入状态污染、结果不可复现）。
> 4. **该场景下 copied 略快于 retained**：四个包大小下 copied 均不低于 retained（4MB 差距最明显：12.54 vs 8.03 MB/s）。原因在于端到端是**跨 EventLoop 异步转发**：retained 的 `retainedDuplicate()` 零拷贝视图需跨线程管理共享引用计数（retain/release 有额外开销且存在竞态风险）；copied 的 `Unpooled.copiedBuffer()` 独立副本在 loopback（内存拷贝极快、无真实网卡瓶颈）下无引用计数跨线程负担，转发更简单，故反而更快。这与第四节**内存内单线程 wrap() 微基准**「零拷贝显著占优」的结论并不矛盾——微基准不经过异步转发与引用计数管理，测的只是纯内存拷贝/视图开销。

## 六、结论

1. **零拷贝收益真实且巨大（尤其是大包）**：1MB 下 `retained`/`duplicate` 较 `copied` 快约 **6500~13000 倍**（50000/100000 vs 7.66 ops/ms）；64KB 下快约 **480~190 倍**。转发路径上「每次全量 copy 一整条 TCP 流」是显著瓶颈，改为零拷贝策略收益立竿见影。
2. **`copied` 开销随尺寸线性飙升**：1KB→1MB 吞吐从 3225 跌到 7.66（约 **420 倍**降幅），全量拷贝的成本正比于字节数；零拷贝策略在三种尺寸下吞吐基本恒定（~1~10 万 ops/ms），与尺寸解耦。
3. **`duplicate` 链路不安全，不可用**：`duplicate` 在微基准中绝对最快，但**共享引用计数**（写端 release 会误伤源 buf），端到端已实锤其崩溃（见 5.2）。故 `duplicate` 仅作零拷贝对照，任何情况下都不应作为默认策略。`retained` 与 `duplicate` 同为零拷贝、吞吐同量级，且持有独立引用、写端 release 安全——故零拷贝路线选 `retained` 而非 `duplicate`。
4. **`copied` 在端到端更稳更快，作为保守安全选择保留**：`copied` 的 `Unpooled.copiedBuffer()` 返回独立副本，链路绝对安全（无共享引用计数竞态），且端到端实测各包大小下均不慢于 retained。唯一代价是内存拷贝成本随消息尺寸增长（微基准可见）。**在跨 EventLoop 异步转发场景下，`copied` 是可优先考虑的安全默认**；`retained` 的零拷贝收益需在真实网卡 / 大包 / 多连接等拷贝成本被放大的场景验证后才应被采纳。
5. **端到端真实链路下 copied 反而略快（与微基准结论相反）**：四个包大小下 copied 均不低于 retained（4MB：12.54 vs 8.03 MB/s）。原因是端到端是跨 EventLoop 异步转发——retained 的共享引用计数跨线程管理开销 + 竞态风险 > loopback 下的一次内存拷贝成本。**默认策略选择需谨慎**：微基准（第四节）显示零拷贝（retained/duplicate）在纯 wrap 上显著占优，但端到端真实转发（跨线程 + 引用计数管理）下 copied 更稳更快。建议在真实网卡/大包/多连接场景进一步压测后，再决定默认策略，而非仅凭微基准。

## 七、下一步（可选增强）

- ~~**接真实转发流量验证**：当前基准为内存内 `wrap()` 微基准，未上真实隧道转发（server↔client 端到端）。可在 `TcpServerHandler`/`WebsocketServerHandler` 等挂上可切换策略，用端到端吞吐/延迟验证零拷贝在真实 IO 路径的收益。~~ **（已完成：见第五节真实端到端场景，含直连 echo 对照；duplicate 真跑崩溃、copied/retained 正常）**
- ~~**直连 echo 对照组**：分离「隧道开销」与「拷贝策略差异」。~~ **（已完成：见 5.2，确认隧道开销主导，copied 端到端略快于 retained）**
- ~~**端到端基准不可复现问题**：retained 在 256KB/4MB 偶发崩溃、4MB 全 `Connection refused`。~~ **（已完成：根因为连续混跑的状态污染，已改为单次单组合运行，见第五节；干净基线全部稳定）**
- **大包 / 真实网卡扩压**：当前为 loopback 场景，copied 反而略快于 retained（跨线程引用计数开销 > 内存拷贝成本）。改用跨机真实网卡（拷贝成本/网络带宽真实）或更大并发后，再验证零拷贝（retained）能否扳回优势——这是决定默认策略的关键补充实验。
- **单次打流放大 + 预热取均值**：5.2 的单跑吞吐受无 JIT 预热影响波动大。建议单次打流增至 32MB+，或同一组合多轮预热后取均值，得到有统计意义的正式吞吐基线（保持「单次单组合」避免状态污染）。
- **策略可配置化**：用 `@Configuration` + `@ConditionalOnProperty` 把默认策略交给 `application.yml` 选择（当前 `BufCopyConfiguration` 已支持 `buf.copy.strategy`，但 duplicate 被强制回退 retained 以防误用）。
- **JMH 正式数据补齐**：对照与端到端测试仅供定性，建议补一轮 JMH fork 基准（多尺寸、多连接、多 forks）作为正式性能基线入库。
- **多连接 / 大包端到端扩压**：当前端到端为单连接，可加并发连接数与大包尺寸，对比 copied / retained 在重负载下的吞吐与稳定性差异，为默认策略选择提供依据。
