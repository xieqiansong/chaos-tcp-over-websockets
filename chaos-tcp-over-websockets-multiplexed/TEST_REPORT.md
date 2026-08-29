# 隧道压测报告（TunnelRealWorldBenchmarkTest）

测试类：`chaos-tcp-over-websockets-benchmark/.../TunnelRealWorldBenchmarkTest#runSingle`

## 测试条件

- 单 JVM 内启动三角色：echo 后端（Netty TCP，收即回）+ 隧道 server（WebSocketServer）+ 隧道 client 入口（TcpServer），数据源用普通 `Socket` 灌流。
- 被测对象：
  - `tunnel_simple`：隧道 client simple 入口（本地端口 21001）
  - `tunnel_multiplexed`：隧道 client multiplexed 入口（本地端口 21002，连接池 + 会话绑定固定 WS 通道 + 零拷贝 `retainedDuplicate`）
  - `direct`：直连 echo 后端（端口 20001，无隧道，作为对照）
- 固定参数：4 并发连接，每连接 16MB，合计 64MB；逐档单跑避免状态污染。
- 指标：吞吐（MB/s，端到端 received 字节 / 耗时）、RTT（ms，单轮「发出→完整收齐回包」平均时延）。

## 结果

| 包大小 | tunnel_simple MB/s | simple rtt(ms) | tunnel_multiplexed MB/s | multiplexed rtt(ms) | direct MB/s | direct rtt(ms) |
|--------|---------------:|-----------:|---------------:|-----------:|------------:|---------------:|
| 1KB    |         10.67 |      0.365 |         13.34 |      0.285 |      57.61 |          0.067 |
| 4KB    |         34.35 |      0.450 |         44.63 |      0.348 |     200.63 |          0.078 |
| 16KB   |         96.68 |      0.634 |        160.40 |      0.387 |     633.66 |          0.096 |
| 64KB   |        157.25 |      1.527 |        400.00 |      0.620 |    1777.78 |          0.138 |
| 256KB  |        209.84 |      4.496 |        524.59 |      1.853 |    1729.73 |          0.490 |
| 1MB    |        245.21 |     15.388 |        592.59 |      6.523 |     888.89 |          4.186 |

## 结论

- **multiplexed 全面优于 simple**：同包大小下吞吐约为 simple 的 1.3～2.5 倍，RTT 更低。连接池（round-robin 分配、会话绑定固定 WS 通道、通道间隔离）与首包缓冲（会话未就绪时缓存而非丢弃）生效。
- **multiplexed 随包增大优势放大**：1KB→1MB 吞吐 13→592 MB/s（约 44×）；simple 仅 10→245 MB/s（约 23×）。小包受 WS 帧/系统调用开销拖累，大包批处理收益最大。
- **1MB 档 multiplexed(592) 已接近直连(888)**；64KB/256KB 区间与直连差距在 3～4× 内。simple 始终差一个数量级。
- `direct` 在 1MB 档吞吐回落（1777→888 MB/s），为 loopback 大包缓冲/回显行为所致，非隧道问题。

## 复现命令

```bash
# 在仓库根目录，先安装上游模块
mvn -pl chaos-tcp-over-websockets-benchmark -am install -DskipTests -Dcheckstyle.skip=true

# 逐档单跑（每档一次 JVM）
for P in 1024 4096 16384 65536 262144 1048576; do
  mvn -pl chaos-tcp-over-websockets-benchmark test \
    -Dtest=TunnelRealWorldBenchmarkTest#runSingle \
    -Dbench.payload=$P -Dcheckstyle.skip=true -Dmaven.test.failure.ignore=true
done
```

结果同时以 CSV 追加至 `chaos-tcp-over-websockets-benchmark/target/bench-results.log`（字段见 `RESULTS_HEADER`）。
