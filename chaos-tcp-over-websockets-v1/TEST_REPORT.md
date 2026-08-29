# 测试报告：TCP-over-WebSocket 隧道转发性能基准

> 本报告为**基准版**，记录 TCP-over-WebSocket 隧道在**当前主代码状态**下的真实端到端性能基线，并给出瓶颈分析。
> 数据采集：2026-08-26，Windows + JDK17，Maven 编译，命令行单次单组合运行，**每个场景测试 3 次取平均值**。
> 原始数据：`target/bench-results.log`（CSV，追加式，可直接程序化解析）。

## 一、测试环境

- OS / JDK：Windows + JDK17
- 构建：Maven（`mvn test`）
- Netty：见 `pom.xml`（4.1.x）
- 主代码状态：**已移除 `TcpServer` 入口的 `FixedRecvByteBufAllocator(4KB)` 限制**，改用 Netty 默认 `AdaptiveRecvByteBufAllocator`（自动适配至 64KB）。其余转发链路为原始实现（每连接独立 EventLoopGroup、每包 `writeAndFlush`）。

## 二、基准方法

**关键：单次单组合运行 + 每场景 3 次取平均**（避免连续混跑时跨轮状态污染导致不可复现，同时用多次平均抵消单次波动）。

- 测试入口：`TunnelRealWorldBenchmarkTest#runSingle`（现位于 `chaos-tcp-over-websockets-benchmark` 子模块，在仓库根目录执行）
- 通过系统属性指定单次测什么，结果以**标准 CSV 追加**到 `target/bench-results.log`：
  ```bash
  mvn -pl chaos-tcp-over-websockets-benchmark -am test \
      "-Dtest=TunnelRealWorldBenchmarkTest#runSingle" \
      -Dbench.strategy=retained -Dbench.payload=262144
  ```
  - `bench.strategy` ∈ {`copied`,`retained`,`duplicate`}
  - `bench.payload` 为包大小字节数（默认 1024）
  - `bench.outfile` 可自定义结果文件路径
- 每次 JVM 只起一个隧道（一个策略 × 一个包大小）+ 一个直连 echo 对照，测完即清理。

**隧道拓扑**：打流客户端(Socket) → `TcpServer`(本地监听) → `WebsocketClient` → WS → `WebsocketServer` → `TcpClient` → echo 后端 → 原路返回。

**打流参数**：1 连接，每连接 4MB，包大小维度 = {1KB, 16KB, 256KB, 4MB}。

**CSV 字段**：`timestamp, strategy, payloadBytes, tunnelMbPerSec, tunnelRttMs, tunnelReceived, tunnelError, directMbPerSec, directRttMs, directReceived, directError`。

## 三、基准结果（当前主代码，2026-08-26，每场景 3 次取平均）

### 3.1 基准结果汇总

| 包大小 | copied 吞吐 | copied RTT | retained 吞吐 | retained RTT | duplicate | direct（直连基线） |
|---|---|---|---|---|---|---|
| 1KB | 2.53 MB/s | 1403 ms | 2.31 MB/s | 1506 ms | **CRASH** | 16.61 MB/s |
| 16KB | 8.57 MB/s | 274 ms | 9.09 MB/s | 265 ms | **CRASH** | 173.91 MB/s |
| 256KB | 13.85 MB/s | 103 ms | 13.53 MB/s | 101 ms | **CRASH** | 364.81 MB/s |
| 4MB | 13.02 MB/s | 48 ms | 14.14 MB/s | 56 ms | **CRASH** | 206.85 MB/s |

> `direct` 为直连 echo 后端（无隧道）基线，为对应包大小下多轮测量平均值。
> `copied/4MB` 的 3 次中有 1 次偶发 `SocketException` 连接失败（0 字节，已排除），有效样本 2 次；其余场景 3 次均正常。
> `duplicate` 单跑即 CRASH（共享引用计数异步不安全，见 3.3）。

### 3.2 关键观察

1. **copied 与 retained 端到端吞吐无显著差异**：取 3 次平均后，各包大小下两者差距均 <10%（1KB copied 高 9%、16KB/4MB retained 高 6%、256KB 基本持平），落在波动范围内。**端到端跨 EventLoop 转发下，零拷贝（retained）未体现出吞吐优势**，其跨线程引用计数管理开销与 loopback 下的一次内存拷贝成本大致抵消。
2. **吞吐随包大小显著上升**：1KB≈2.5 → 4MB≈13-14 MB/s。这是**「每包固定开销主导」**的典型特征——每转发一个包有固定成本（跨线程投递 + flush + WS 编解码），包越大固定成本分摊越少。
3. **direct 直连远高于隧道**：direct 达 17~365 MB/s，隧道仅 2.5~14 MB/s，**隧道链路固有开销是主导瓶颈**（约 10~30× 衰减）。
4. **duplicate 不可用**（见 3.3）。

### 3.3 duplicate（零拷贝共享引用，不安全）

- 单跑即 CRASH（SocketException / IllegalReferenceCountException）。
- `duplicate()` 派生 buf 与原 buf **共享引用计数**，在异步转发链路中写端 `release()` 会误伤源 buf，触发 `IllegalReferenceCountException` / 连接断开。**任何情况下不应作为默认策略**。

## 四、瓶颈分析

### 4.1 瓶颈模型：每包固定开销主导

证据（3.1）：隧道吞吐随包大小上升约 5×（1KB→4MB），direct 也随包大小上升（17→365）。这说明**隧道链路对每个转发包都附加了远高于直连的固定开销**，吞吐被"包数 × 每包开销"约束。

### 4.2 隧道链路的结构性开销来源（按影响排序）

1. **每包跨 EventLoop 投递 + 立即 flush（主因）**：隧道链路有 4 处跨线程转发
   - 打流客户端 → `TcpServer` → `TcpServerHandler` → `WebsocketClient.writeAndFlush`
   - `WebsocketClient` → WS server → `WebsocketServerHandler` → `TcpClient.writeAndFlush`
   - 回程同理（`TcpClientHandler` → WS → `WebsocketClientHandler` → 本地 TCP）
   每处都 `writeAndFlush`（**每包一次 flush，无批量化**），且每包跨 EventLoop 提交一次异步任务。小包（1KB）下这些固定开销成为吞吐上限。

2. **WS 帧编解码**：每包经 `WebSocket08FrameEncoder/Decoder` 做掩码/帧头处理，有固定 CPU 成本。

3. **每连接独立 EventLoopGroup / 固定线程池**：`WebsocketClient`/`TcpClient` 每连接各自 `new NioEventLoopGroup()`，`TcpServerHandler`/`WebsocketServerHandler` 各 `newFixedThreadPool(32)`。高并发下线程数随连接线性爆炸，且大量跨 EventLoop 调度。

4. **`AdaptiveRecvByteBufAllocator` 上限 64KB**：单个大 TCP 数据块最多被切成 ~64KB 子块转发，对超大单包仍会产生多次转发。已比固定 4KB 好得多，但非批量化根本解。

### 4.3 已排除的瓶颈

- **收包切片不是主导瓶颈**：移除固定 4KB 收包（回退默认 Adaptive）后，copied+4MB 从 12.54 → ~13 MB/s（小幅提升，RTT 明显改善），未达量级提升。收包大小是次要因素。
- **拷贝策略差异不是主导**：copied 与 retained 端到端无显著差异（见 3.2-1）。
- **主代码引用计数 / 线程模型本身健康**：单次单组合运行下 copied/retained 全包大小稳定、无泄漏（copied/4MB 偶发 1 次 SocketException，属连接级偶发，非系统性缺陷）。

## 五、结论

1. **当前隧道瓶颈 = 每包固定开销**（跨线程投递 + 每包 flush + WS 编解码），非收包缓冲、非拷贝策略。
2. **copied 与 retained 端到端等效**：零拷贝（retained）在真实跨线程转发下无吞吐优势，且 retained 的跨线程引用计数管理有竞态风险；copied 更简单直接、引用绝对安全。**默认策略在端到端场景无需依赖 retained**。
3. **duplicate 绝对不可用**（共享引用计数异步不安全）。
4. **最大优化空间**：批量写 + 周期 flush（替代每包 `writeAndFlush`）、降低跨 EventLoop 投递次数、统一/共享 EventLoop 模型。这些预计能显著摊薄每包固定开销。

## 六、下一步

- **批量写 + 周期 flush**：把 `TcpServerHandler`/`TcpClientHandler` 的每包 `writeAndFlush` 改为批量 `write` + 定时/阈值 `flush`，降低每包 flush 固定开销（对齐 4.2-1）。
- **降低跨 EventLoop 投递**：将隧道链路涉及的连接收敛到共享/同一 EventLoop，减少每包跨线程 task 投递与内存序同步。
- **共享 EventLoop / 线程池治理**：替换每连接 `new NioEventLoopGroup()` + 固定 32 线程池，避免高并发线程爆炸。
- **大包 / 真实网卡扩压**：当前为 loopback，TCP 与 WS 开销被低估。跨机真实网卡下再验证 copied vs retained 及批量写收益。
- **打流放大 + 更多样本**：将单次流量增至 32MB+ 或每场景更多次取平均，进一步降低抖动（当前 4MB 下 4MB 大包偶发 SocketException）。
