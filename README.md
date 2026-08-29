# chaos-tcp-over-websockets

基于 [Netty](https://netty.io/) 的 **TCP over WebSocket** 隧道工具集合（Maven 多模块），可在仅开放 WebSocket 端口的受限网络环境下，将任意 TCP 流量（SSO 数据库、内网服务等）经 WebSocket 协议中转穿透。

## 模块组成

| 模块 | 定位 | 一句话说明 |
|---|---|---|
| [`chaos-tcp-over-websockets-simple`](chaos-tcp-over-websockets-simple/README.md) | 简单直连版 | URL 路由目标地址、单条 WS 连接、二进制帧透传，Spring Boot 开箱即用 |
| [`chaos-tcp-over-websockets-multiplexed`](chaos-tcp-over-websockets-multiplexed/README.md) | 会话多路复用版 | WS 连接池 + 会话绑定固定通道 + 自定义协议 + 零拷贝，面向高并发高吞吐 |
| [`chaos-tcp-over-websockets-benchmark`](chaos-tcp-over-websockets-benchmark/README.md) | 性能基准 | JMH 微基准 + 端到端隧道压测，不参与业务发布 |

两个隧道实现是**并列的两种架构路线**，不是"新旧替代"关系，各有适用场景。

## 如何选型

| 维度 | simple | multiplexed |
|---|---|---|
| 数据面 | 裸 WebSocket 二进制帧透传 | 自定义协议（8B sessionId + 4B length + payload） |
| 控制面 | 无，目标地址由 URL `/forward/{host}/{port}` 指定 | JSON 控制消息（open / opened / openFailed / close） |
| WS 连接 | 单条 | 连接池（默认 4 条）+ 断线自动重连 |
| 会话 | 无，一连接一转发 | Session 管理，会话绑定固定 WS 通道，通道间隔离 |
| 拷贝策略 | `buf.copy.strategy` 可配（copied / retained） | 零拷贝（`CompositeByteBuf` + `retain`） |
| 启动方式 | 可执行 jar：`server` / `client` 两个命令 | 库 + 演示 main（IDE 运行 `Server` / `TcpServer`） |
| 端到端吞吐（1KB→1MB） | ~10 → ~245 MB/s | ~13 → ~592 MB/s |
| 适合场景 | 快速部署、连接数不高、希望一条命令跑起来 | 高并发、多连接复用同一批 WS 通道、追求吞吐 |

> 性能数据来自 `chaos-tcp-over-websockets-benchmark` 的端到端压测，详见 [multiplexed/TEST_REPORT.md](chaos-tcp-over-websockets-multiplexed/TEST_REPORT.md)。

## 快速开始

### 构建

```bash
# 构建全部模块（simple 产出可执行 jar，benchmark 产出 JMH uber jar）
mvn clean package -DskipTests
```

### 使用 simple（简单直连）

```bash
# server 端：监听 WebSocket，供 client 连入
java -jar chaos-tcp-over-websockets-simple/target/tcp-over-websockets-simple-exec.jar server 7002

# client 端：本地监听 13306，隧道转发到远端 127.0.0.1:3306
java -jar chaos-tcp-over-websockets-simple/target/tcp-over-websockets-simple-exec.jar \
    client 13306 ws://server-host:7002/forward/127.0.0.1/3306
```

### 使用 multiplexed（会话多路复用）

目前为库模块 + 演示 main，建议在 IDE 中运行，或经 benchmark 压测驱动：

- 启动 server：运行 `lan.chaos.modules.tcp.over.websockets.multiplexed.server.Server`（默认监听 7002）
- 启动 client 入口：运行 `lan.chaos.modules.tcp.over.websockets.multiplexed.client.TcpServer`（参数：`wsUrl` `poolSize`，本地端口 21002 与目标 `127.0.0.1:20001` 为演示硬编码）

### 跑基准

```bash
# JMH 微基准（ByteBuf 拷贝策略）
java -jar chaos-tcp-over-websockets-benchmark/target/benchmarks.jar BufCopyStrategyBenchmark

# 端到端隧道压测（需先启动 simple / multiplexed 隧道服务与 echo 后端）
mvn -pl chaos-tcp-over-websockets-benchmark test -Dtest=TunnelRealWorldBenchmarkTest#runSingle -Dbench.payload=65536
```

详见各子模块 README 与 [benchmark/TEST_REPORT 说明](chaos-tcp-over-websockets-benchmark/README.md)。

## 目录结构

```
chaos-tcp-over-websockets/
├── pom.xml                                        # 父 POM（spring-boot-starter-parent 2.7.18，聚合三个模块）
├── chaos-tcp-over-websockets-simple/              # 简单直连版隧道（Spring Boot，可执行 jar）
│   ├── README.md
│   └── TEST_REPORT.md
├── chaos-tcp-over-websockets-multiplexed/         # 会话多路复用版隧道（库 + 演示 main）
│   ├── README.md
│   └── TEST_REPORT.md
├── chaos-tcp-over-websockets-benchmark/           # 性能基准（JMH 微基准 + 端到端压测）
│   └── README.md
└── LICENSE                                        # MIT
```

## 技术要点

- **Netty 4.1**（netty-all 4.1.115.Final），NIO / Epoll 自适应（Windows 用 NIO，Linux 用 Epoll）。
- 两个隧道模块均采用**共享 EventLoopGroup**（boss + CPU×2 worker，引用计数管理），避免每连接线程爆炸。
- simple 的 `buf.copy.strategy` 支持 `copied`（全量拷贝）/ `retained`（零拷贝 + 引用计数，默认）/ `duplicate`（不安全，强制回退 retained）。
- multiplexed 采用 `CompositeByteBuf` 零拷贝编码数据帧，并实现首包缓冲、断线指数退避重连、会话通道隔离。

## 参考来源

- 本项目参考实现了 [995270418L/Tcp_Over_websockets](https://github.com/995270418L/Tcp_Over_websockets) 的 Netty TCP/WebSocket 编解码思想，并在此基础上重构为 client/server 隧道模式、按目标地址转发、适配 Windows/Linux 原生传输层。

## License

[MIT](LICENSE)（参考来源项目保留其原始版权与协议）。
