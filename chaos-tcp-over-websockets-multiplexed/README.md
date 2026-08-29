# chaos-tcp-over-websockets-multiplexed

基于 [Netty](https://netty.io/) 实现的 **TCP over WebSocket** 隧道工具（**会话多路复用版**），通过 WebSocket 连接池 + 会话绑定 + 自定义协议，把大量本地 TCP 连接复用到少数几条 WS 通道上，面向高并发、高吞吐场景。

> **模块定位**：本模块属于 `chaos-tcp-over-websockets` 项目集合中的 **multiplexed（多路复用）** 路线。与追求一条命令跑起来的兄弟模块 [`chaos-tcp-over-websockets-simple`](../chaos-tcp-over-websockets-simple/README.md)（URL 路由、单连接、简单直连）是并列关系；当并发连接多、希望复用通道、追求吞吐时优先选本模块。

## 与 simple 版的差异

| 维度 | simple | multiplexed |
|---|---|---|
| 数据面 | 裸 WebSocket 二进制帧透传 | 自定义协议（`[8B sessionId][4B length][payload]`） |
| 控制面 | 无，目标由 URL `/forward/{host}/{port}` 指定 | JSON 控制消息（open / opened / openFailed / close） |
| WS 连接 | 单条 | 连接池（默认 4 条）+ 断线自动重连（指数退避，上限 2s） |
| 会话 | 无，一连接一转发 | Session 管理，会话绑定固定 WS 通道，通道间隔离 |
| 数据拷贝 | `buf.copy.strategy` 可配 | 零拷贝编码（`CompositeByteBuf` + `retain`） |
| 端到端吞吐（1KB→1MB） | ~10 → ~245 MB/s | ~13 → ~592 MB/s |

> 性能数据来自 `chaos-tcp-over-websockets-benchmark` 端到端压测，完整报告见 [TEST_REPORT.md](TEST_REPORT.md)。

## 架构设计

```
client 端（本地）                          server 端
┌────────────────────────┐   WS 连接池    ┌──────────────────────────┐
│ 本地 TCP 监听 (TcpServer) │ ─────────────► │ WebSocket Server (Server) │
│   │ 每个 TCP 连接          │  open(host,port) │   │ 按目标建 TCP 会话        │
│   ▼ 绑定到池中某条 WS 通道    │  opened(sessionId)│   ▼                        │
│ Client（连接池 + 会话路由）  │ ◄───────────── │ SessionManager            │
└────────────────────────┘   数据帧(二进制)   └──────────────────────────┘
```

核心机制：

1. **WS 连接池**：client 启动时建立 `poolSize` 条（默认 4）到 server 的 WebSocket 连接；新的本地 TCP 会话按 round-robin 分配到其中一条。
2. **会话绑定固定通道**：一个会话（sessionId）建立后，其全部数据帧都走同一条 WS 通道。多条通道互相隔离——某条通道抖动/断开只影响落在它上面的会话，不波及其它通道上的连接。
3. **首包缓冲**：本地 TCP 首包到达时会话可能尚未建立（open → opened 有往返），此时数据先入队缓存而非丢弃，`opened` 回填后统一 flush，保证顺序且不丢首包。
4. **断线自动重连**：某条 WS 断开后，其上的会话被关闭清理，通道按指数退避（200ms 起步、上限 2s）自动重连补齐池容量，不影响其它通道。
5. **零拷贝**：数据帧编码用 `Unpooled.wrappedBuffer(header, payload.retain())` 组合 header 与 payload，不复制数据本体；写端负责 `release()`。
6. **共享 EventLoopGroup**：`SharedEventLoopGroups` 全局共享一组 boss/worker（引用计数管理，最后一个持有方关闭时才真正释放），避免每连接线程爆炸。

## 协议

### 控制面（文本帧，JSON）

| 消息 | 方向 | 字段 |
|---|---|---|
| `open` | client → server | `type`、`host`、`port`、`requestId`（client 生成，用于匹配） |
| `opened` | server → client | `type`、`sessionId`、`requestId`（原样回带） |
| `openFailed` | server → client | `type`、`sessionId`、`requestId`（目标连不上时返回，client 关闭本地 TCP） |
| `close` | 双向 | `type`、`sessionId` |

### 数据面（二进制帧）

```
+------------+----------+------------------+
| sessionId  |  length  |     payload      |
| (8 bytes)  | (4 bytes)|  (length bytes)  |
+------------+----------+------------------+
```

- `sessionId`：long 大端，标识会话，用于路由回本地 TCP。
- `length`：int 大端，payload 字节数，用于流式 TCP/WS 上界定包边界（拆包/粘包）。

## 用法

当前为**库模块 + 演示 main**（未配置可执行 jar），建议在 IDE 中直接运行，或由 `chaos-tcp-over-websockets-benchmark` 压测驱动。

```bash
# 构建
mvn -pl chaos-tcp-over-websockets-multiplexed -am package -DskipTests
```

### 启动 server

运行 `lan.chaos.modules.tcp.over.websockets.multiplexed.server.Server` 的 `main`：

```bash
# 默认端口 7002，可传端口覆盖
Server.main()   # 或 Server.main("9000")
```

### 启动 client 入口

运行 `lan.chaos.modules.tcp.over.websockets.multiplexed.client.TcpServer` 的 `main`：

```bash
# 参数：[wsUrl] [poolSize]
#   wsUrl    默认 ws://localhost:7002
#   poolSize 默认 4
# 注意：本地监听端口 21002 与目标 127.0.0.1:20001 为演示硬编码，业务接入前需按需调整
TcpServer.main("ws://server-host:7002", "4")
```

> 完整业务接入建议以 `Client.connect(wsUrl, poolSize)` + `TcpServer.start(localPort, targetHost, targetPort)` 两个 API 组合编程（参见 `TcpServer.main` 的调用方式）。

## 代码结构

```
src/main/java/lan/chaos/modules/tcp/over/websockets/multiplexed/
├── client/
│   ├── Client.java                  # 核心：WS 连接池 + 会话路由 + 首包缓冲 + 断线重连
│   ├── TcpServer.java               # 本地 TCP 监听入口（含演示 main）
│   ├── TcpServerHandler.java        # 本地 TCP 连接 → open 会话 → 转发数据
│   └── WebSocketClientHandler.java  # WS 握手 + 控制/数据帧分发
├── server/
│   ├── Server.java                  # WebSocket 服务端（含 main）
│   ├── WebSocketUpgradeHandler.java # WS 升级握手 + 控制消息处理 + 数据帧路由
│   └── TargetTcpHandler.java        # 目标 TCP 连接（收到的数据编码回帧）
├── protocol/
│   ├── ControlMessage.java          # 控制消息模型（open/opened/openFailed/close）
│   ├── ControlMessageCodec.java     # JSON 编解码
│   └── DataFrameCodec.java          # 二进制数据帧编解码（零拷贝 encodeZeroCopy）
├── session/
│   ├── Session.java                 # 会话（sessionId + 目标地址 + WS 通道 + 目标 TCP 通道）
│   └── SessionManager.java          # 会话生命周期管理
└── util/
    └── SharedEventLoopGroups.java   # 全局共享 EventLoopGroup（引用计数）
```

## 性能摘要

端到端压测（4 并发连接 × 每连接 16MB，loopback，见 [TEST_REPORT.md](TEST_REPORT.md)）：

| 包大小 | simple | multiplexed | direct（直连） |
|---|---:|---:|---:|
| 1KB | 10.67 MB/s | 13.34 MB/s | 57.61 MB/s |
| 64KB | 157.25 MB/s | 400.00 MB/s | 1777.78 MB/s |
| 1MB | 245.21 MB/s | 592.59 MB/s | 888.89 MB/s |

- multiplexed 同包大小下吞吐约为 simple 的 1.3～2.5 倍，RTT 更低。
- 1MB 档 multiplexed（592 MB/s）已接近直连（888 MB/s）；64KB/256KB 区间与直连差距在 3～4× 内。

## 现状与限制

- `Server.main` / `TcpServer.main` 为演示骨架：`TcpServer.main` 的本地端口与目标地址为配合 echo 压测硬编码，生产接入需按业务场景传入实际参数。
- 模块不产出可执行 jar（无 Spring Boot / shade 插件），以库形式供业务或压测模块引用。
- 数据帧采用零拷贝编码，调用方需遵循 Netty 引用计数约定（`retain` / `release` 配对），避免泄漏。

## License

[MIT](../LICENSE)（参考来源项目保留其原始版权与协议）。
