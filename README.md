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
# 构建可执行 jar
mvn clean package

# 1) server 端：监听 WebSocket，供 client 连入
java -jar target/tcp-over-websockets.jar server 7002

# 2) client 端：本地监听 13306，隧道转发到远端目标 127.0.0.1:3306
java -jar target/tcp-over-websockets.jar client 13306 ws://server-host:7002/forward/127.0.0.1/3306
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
java -jar target/tcp-over-websockets.jar client 13306 ws://tunnel.example.com/forward/127.0.0.1/3306
```

> 若企业仅开放 HTTPS（443），在 `server` 块增加 443/SSL 监听，client 端把 `ws://` 换成 `wss://` 即可，其余配置不变。

## 构建

- JDK 8+
- Maven

依赖自动下载（netty-all / commons-collections4 / logback / lombok）。

## 参考来源

- 本项目参考实现了 [995270418L/Tcp_Over_websockets](https://github.com/995270418L/Tcp_Over_websockets)（Public，无独立 LICENSE 声明）的 Netty TCP/WebSocket 编解码思想，并在此基础上重构为 client/server 隧道模式、按目标地址转发、适配 Windows/Linux 原生传输层（NIO/Epoll）。

## License

[MIT](LICENSE)（参考来源项目保留其原始版权与协议）。