# chaos-tcp-over-websockets

基于 [Netty](https://netty.io/) 实现的 **TCP over WebSocket** 隧道工具，可在网络受限的环境下通过 WebSocket 协议中转任意 TCP 流量（如 SSO 数据库、内网服务等）。

- 采用 `client / server` 双端模式，由 `server` 端统一桥接 WebSocket 与目标 TCP 服务。
- 数据以 WebSocket 二进制帧在两端透传，链路保持/心跳由 Netty 处理。
- 参照原项目改造：将原「websocket ⇄ tcp 互相转换」重构为按 URL 指定转发目标的隧道模式。

## 原理

```
client 端                           server 端
┌─────────────────┐  WebSocket   ┌─────────────────────────────────┐
│ 本地 TCP 监听端口  │ ◄──────────► │  WebSocket Server(ws://host:port) │
│ 收到连接后转发       │  二进制帧     │  按 /forward/{host}/{port} 发起     │
└─────────────────┘              │  到目标服务的 TCP 连接              │
                                 └─────────────────────────────────┘
```

以 client 连接 `ws://server:7002/forward/127.0.0.1/3306` 为例，client 本地开放端口收到的数据，会经 WebSocket 隧道转发到 server 背后可达的 `127.0.0.1:3306`，实现无直连网络下的端口穿透。

## 用法

```bash
# 构建：产出普通 jar（供其他模块依赖）与 -exec 可执行 jar
mvn clean package

# 1) server 端：监听 WebSocket，供 client 连入
java -jar target/tcp-over-websockets-exec.jar server 7002

# 2) client 端：本地监听 13306，隧道转发到远端目标 127.0.0.1:3306
java -jar target/tcp-over-websockets-exec.jar client 13306 ws://server-host:7002/forward/127.0.0.1/3306
```

> server 端默认转发地址以 `ws://localhost:7002` 兜底；client 端必须指定完整的 WebSocket 地址。

## 受限网络部署（Nginx 前端转发）

企业端口受限时往往只开放一个前端端口，可在这一个端口上用 Nginx 把 WebSocket 升级请求转发到内网 server 端。

以 server 端监听 `127.0.0.1:7002`、企业仅开放前端 80 端口为例：

```nginx
server {
    listen 80;
    server_name tunnel.example.com;   # 企业唯一开放的前端域名

    # 隧道转发：/forward/ 路径整体透传给内网 server 端
    location /forward/ {
        proxy_pass http://127.0.0.1:7002;

        proxy_http_version 1.1;

        # 透传 WebSocket 升级头，缺失会导致握手失败
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        # 实时双向转发，禁用缓冲以避免额外内存占用与延迟
        proxy_buffering off;
        proxy_request_buffering off;

        # 隧道为长连接，读写超时不宜设得过短
        proxy_connect_timeout 60s;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
```

此时 client 端只需指向该前端端口即可：

```bash
java -jar target/tcp-over-websockets-exec.jar client 13306 ws://tunnel.example.com/forward/127.0.0.1/3306
```

> 若企业仅开放 HTTPS（443），在 `server` 块增加 443/SSL 监听，client 端把 `ws://` 换成 `wss://` 即可，其余配置不变。

## 性能基准（JMH）

隧道转发路径上的核心开销是 `ByteBuf` 的拷贝策略。当前实现每个转发环节都调用 `Unpooled.copiedBuffer(...)` 做一次**全量拷贝**，本仓库在 `chaos-tcp-over-websockets-benchmark` 子模块内用 [JMH](https://github.com/openjdk/jmh) 对三种策略做了微基准对拍（单线程、Throughput、ops/s）：

| 策略 | 1KB | 64KB | 1MB |
|---|---|---|---|
| `copiedBuffer`（全量拷贝，当前实现） | ~9.7M | ~134K | ~8.5K |
| `duplicate`（零拷贝视图，不计数） | ~200M | ~197M | ~199M |
| `retainedDuplicate`（零拷贝 + 引用计数） | ~51M | ~51M | ~51M |

**结论**：`copiedBuffer` 的吞吐随消息尺寸几乎线性下降（1KB → 1MB 掉了约 1000 倍），而零拷贝系列与消息尺寸无关。当隧道承载大包（如二进制、数据库大结果集）时，`retainedDuplicate` / `duplicate` 相比全量拷贝有数量级优势，是最值得做的零拷贝优化方向。

> 说明：`duplicate` 的 ~170M 含 JVM 编译器逃逸分析/标量替换的假象，且其视图**不持有引用计数**，真实转发中底层 ByteBuf 一旦被 `release()` 即会读写悬空，故**不宜采用**，仅供对比观察；`retainedDuplicate` 会真实递增 refCnt（CAS，JIT 无法消除），数字可信且不随尺寸下降，是迁移零拷贝的推荐策略，同时需在写端配合 `release()` 做好生命周期管理。

```bash
# 方式一（推荐）：在仓库根目录打包 uber jar 后运行，JMH 可正常 fork 出干净 JVM
mvn -pl chaos-tcp-over-websockets-benchmark -am package -DskipTests
java -jar chaos-tcp-over-websockets-benchmark/target/benchmarks.jar BufCopyStrategyBenchmark

# 方式二：免打包，进程内快速验证
mvn -pl chaos-tcp-over-websockets-benchmark -am compile exec:java \
    "-Dexec.mainClass=org.openjdk.jmh.Main" "-Dexec.args=BufCopyStrategyBenchmark -f 0"
```

> 说明：基准代码已迁移到独立子模块 `chaos-tcp-over-websockets-benchmark`（`src/main` 放 JMH 微基准，`src/test` 放端到端隧道压测），不再进入 v1 的主发布 jar。exec 环境下 JMH 子进程 fork 依赖自身 classpath，故方式二使用 `-f 0` 进程内运行，仅供快速验证；正式性能数据请用方式一。

## 构建

- JDK 8+
- Maven

依赖自动下载（netty-all / commons-collections4 / logback / lombok；JMH 仅用于 `chaos-tcp-over-websockets-benchmark` 子模块）。

## 参考来源

- 本项目参考实现了 [995270418L/Tcp_Over_websockets](https://github.com/995270418L/Tcp_Over_websockets)（Public，无独立 LICENSE 声明）的 Netty TCP/WebSocket 编解码思想，并在此基础上重构为 client/server 隧道模式、按目标地址转发、适配 Windows/Linux 原生传输层（NIO/Epoll）。

## 变更记录

### 2026-08-28：共享 EventLoopGroup（线程资源治理）

- **改动**：隧道链路中 `WebsocketClient` / `TcpClient` 原本每次建连 `new NioEventLoopGroup()` 且从不 `shutdown()`，叠加 `TcpServerHandler` / `WebsocketServerHandler` 各 `newFixedThreadPool(32)`，导致**每连接线程数随连接数线性爆炸且不回收**。现改为全局共享一组 `NioEventLoopGroup`（1 个 boss + CPU×2 worker），由引用计数（`AtomicInteger` 持有方）统一在最后一个连接关闭时 `shutdownGracefully()`。
- **效果**：
  - 消除线程泄漏，线程数从「随连接爆炸」收敛为「固定一组」；
  - 端到端吞吐 / RTT 与优化前基线**持平、无回归**（实测 copied 策略 1KB~4MB 全包大小稳定跑完，小包 RTT 抖动在 run-to-run 正常范围内）。
- **说明**：本改动为资源治理性质，不消除 4 跳跨 EventLoop 调度的固有延迟，因此小包延迟不会系统性改善；吞吐收益来自消除线程爆炸后的系统稳定性，而非批量/零拷贝层面的优化。

## License

[MIT](LICENSE)（参考来源项目保留其原始版权与协议）。