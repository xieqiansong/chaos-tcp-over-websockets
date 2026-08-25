# 测试报告：隧道转发 ByteBuf 拷贝策略（copiedBuffer / duplicate / retainedDuplicate）

> 统一报告：覆盖「环境准备 → 对照测试 → 三实现吞吐实测 → 真实端到端场景 → 结论」。
> 数据采集：2026-08-25，Windows + JDK8（本机运行 JDK8），Maven 编译，单线程 IDEA 点运行。

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

## 四、三实现吞吐实测（IDEA 对照测试，2026-08-25）

> 单位 ops/ms（每毫秒完成的 wrap 次数，越高越快）。对照测试无 JVM 预热控制，绝对值仅供定性，重点看策略间相对量级。每格 200,000 次循环。

| size（字节） | copied（全量拷贝） | retained（零拷贝 v2） | duplicate（零拷贝 v1，不安全） |
|---|---|---|---|
| 1024 | 3225.81 | 13333.33 | 40000.00 |
| 65536 | 104.22 | 50000.00 | 20000.00 |
| 1048576 | 7.66 | 50000.00 | 100000.00 |

附本轮原始耗时（ms / 20 万次）：1024 → copied 62 / retained 15 / duplicate 5；65536 → copied 1919 / retained 4 / duplicate 10；1MB → copied 26121 / retained 4 / duplicate 2。

## 五、真实隧道端到端场景（方案 B，2026-08-25）

> 目的：把三策略放到**真实异步转发链路**验证，而非内存内 `wrap()` 微基准。拓扑：echo 后端(Netty TCP，收到即回) + 隧道 server(`WebsocketServer`) + 隧道 client 入口(`TcpServer`) + 打流客户端(普通 Socket)。场景实现见 [TunnelRealWorldBenchmarkTest.java](file:///d:/project/chaos/chaos-tcp-over-websockets/src/test/java/lan/chaos/modules/tcp/over/websockets/benchmark/TunnelRealWorldBenchmarkTest.java)，IDEA 点一下跑完整四轮（含直连对照）。
> 注：本测试直接 `new XxxStrategy()` 注入两段转发 handler，**绕过 `BufCopyConfiguration` 对 duplicate 的回退**，使 duplicate 真跑，验证其异步链路不安全。
> **关键前提**：测试期通过 `src/test/resources/logback-test.xml` 将日志降级为 WARN。**logback 默认 root=DEBUG 会大量打印 Netty 内部日志，在 Windows 同步写控制台时严重抢占 EventLoop 线程、压低基准**——实测开启 DEBUG 时吞吐被腰斩（见下"日志级别影响"），故正式数据均在关 DEBUG 下采集。

### 5.1 并发参数与结果（4 并发连接，每连接 16MB，合计 64MB）

| 策略 | 端到端吞吐 | 回收字节 | RTT | 结果 |
|---|---|---|---|---|
| **direct（直连 echo，无隧道）** | 50.27 MB/s | 67108864 / 67108864 | 1271 ms | 正常（基线） |
| copied（全量拷贝 + 隧道） | 8.09 MB/s | 67108864 / 67108864 | 7651 ms | 正常 |
| retained（零拷贝 v2 + 隧道） | 8.87 MB/s | 67108864 / 67108864 | 7042 ms | 正常 |
| duplicate（零拷贝 v1 + 隧道，不安全） | — | 0 / 67108864 | — | **CRASH** |

### 5.2 数据解读

- **隧道固有开销是主导**：直连 echo 达 **50.27 MB/s**，经隧道（retained）仅 **8.87 MB/s**——隧道（WS 封装 + 双向 Netty 转发 + loopback 双重 TCP）带来约 **5.7× 吞吐衰减**，这远大于拷贝策略之间的差异。也就是说在当前小包 + loopback 场景下，"隧道"才是瓶颈，"拷贝策略"是次要因素。
- **隧道内零拷贝仍占优**：retained(8.87) 较 copied(8.09) 快 **~1.10×**，RTT 更低（7042 vs 7651 ms）。优势被隧道开销稀释（拷贝成本占比小），但方向正确且真实；在去隧道瓶颈的大包 / 高带宽场景，微基准已证明零拷贝优势会放大到数百~上千倍。
- **duplicate 真跑崩溃**：日志 `io.netty.util.IllegalReferenceCountException` / `SocketException` / `SocketTimeoutException`——`duplicate()` 派生 buf `release()` 把共享引用计数减到 0，异步写时源 buf 已释放，连接被 RST / 读超时，回包 0 字节。**直连组无此问题**（不经过 WS 转发，无引用计数错乱），进一步坐实病根为"异步转发链路 + 共享引用计数"。

### 5.3 日志级别影响（踩坑记录）

| 配置 | copied | retained | 说明 |
|---|---|---|---|
| 开 DEBUG（logback 默认） | 2.42 MB/s | 2.77 MB/s | Netty DEBUG 刷屏抢占 EventLoop，吞吐被腰斩 |
| 关 DEBUG（WARN） | 8.09 MB/s | 8.87 MB/s | 正常基线，约为开 DEBUG 的 **3.3×** |

> 结论：端到端基准务必关闭 DEBUG 日志，否则数据严重失真。本报告的 5.1 数据均为关 DEBUG 下采集。

## 六、结论

1. **零拷贝收益真实且巨大（尤其是大包）**：1MB 下 `retained`/`duplicate` 较 `copied` 快约 **6500~13000 倍**（50000/100000 vs 7.66 ops/ms）；64KB 下快约 **480~190 倍**。转发路径上「每次全量 copy 一整条 TCP 流」是显著瓶颈，改为零拷贝策略收益立竿见影。
2. **`copied` 开销随尺寸线性飙升**：1KB→1MB 吞吐从 3225 跌到 7.66（约 **420 倍**降幅），全量拷贝的成本正比于字节数；零拷贝策略在三种尺寸下吞吐基本恒定（~1~10 万 ops/ms），与尺寸解耦。
3. **默认选 `retainedDuplicate` 而非 `duplicate`**：`duplicate` 在 1KB/1MB 绝对最快，但**共享引用计数、链路不安全**（写端 release 会误伤源 buf）；`retained` 同样零拷贝、吞吐同量级，且持有独立引用、写端 release 安全。真实端到端已实锤 duplicate 崩溃，故 retained 为默认策略。
4. **`copied` 作为安全回退保留**：仅在"下游必须持有独立副本、且不可承受引用计数约束"的极端场景使用；常规隧道转发一律走 `retained`。
5. **隧道固有开销远大于拷贝差异（真实场景新认知）**：小包 + loopback 下隧道本身带来 ~5.7× 衰减，拷贝策略仅在隧道内产生 ~1.1× 差异。若要凸显零拷贝优势，应在大包 / 真实网卡（去 loopback 瓶颈）场景压测，或在微基准（第四节）层面观察。

## 七、下一步（可选增强）

- ~~**接真实转发流量验证**：当前基准为内存内 `wrap()` 微基准，未上真实隧道转发（server↔client 端到端）。可在 `TcpServerHandler`/`WebsocketServerHandler` 等挂上可切换策略，用端到端吞吐/延迟验证零拷贝在真实 IO 路径的收益。~~ **（已完成：见第五节真实端到端场景，含直连 echo 对照；retained 较 copied 端到端快 ~1.1×，duplicate 真跑崩溃）**
- ~~**直连 echo 对照组**：分离「隧道开销」与「拷贝策略差异」。~~ **（已完成：direct 50.27 MB/s vs 隧道 8.87 MB/s，确认隧道开销主导）**
- **大包 / 真实网卡扩压**：当前为 1KB 小包 + loopback，隧道开销掩盖拷贝差异。改用 64KB/1MB 大包或跨机真实网卡，零拷贝优势预计放大（与微基准结论呼应）。
- **策略可配置化**：用 `@Configuration` + `@ConditionalOnProperty` 把默认策略交给 `application.yml` 选择（当前 `BufCopyConfiguration` 已支持 `buf.copy.strategy`，但 duplicate 被强制回退 retained 以防误用）。
- **JMH 正式数据补齐**：对照与端到端测试仅供定性，建议补一轮 JMH fork 基准（多尺寸、多连接、多 forks）作为正式性能基线入库。
- **JMH 正式数据补齐**：对照测试与端到端测试仅供定性，建议补一轮 JMH fork 基准（多尺寸、多连接、多 forks）作为正式性能基线入库。
- **多连接 / 大包端到端扩压**：当前端到端为单连接 1KB 小包，可加并发连接数与大包尺寸，观察零拷贝优势在重负载下的放大倍数。
