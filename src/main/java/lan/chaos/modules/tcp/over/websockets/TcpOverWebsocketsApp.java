package lan.chaos.modules.tcp.over.websockets;

import lan.chaos.modules.tcp.over.websockets.client.TcpServer;
import lan.chaos.modules.tcp.over.websockets.server.WebsocketServer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class TcpOverWebsocketsApp {

    /*
    example server (本地监听 7002, 供 client 连入):
        server 7002
    example client (本地监听 13306, 转发到远端 websocket 服务器及其背后的目标服务):
        client 13306 ws://example.com:7002/forward/<target-host>/3306
     */
    public static void main(String[] args) {
        String type;
        int port;
        String wsUrl;
        // 校验参数
        try {
            type = args[0];
            port = Integer.parseInt(args[1]);
            wsUrl = Objects.equals("client", type) ? args[2] : "ws://localhost:7002";
        } catch (Exception e) {
            System.out.println("example server:\n"
                    + "    java -jar app.jar server 7002\n"
                    + "example client:\n"
                    + "    java -jar app.jar client 13306 ws://example.com:7002/forward/<target-host>/3306");
            return;
        }
        if (Objects.equals("server", type)) {
            @Cleanup
            WebsocketServer server = new WebsocketServer();
            server.start(port);
        }
        if (Objects.equals("client", type)) {
            @Cleanup
            TcpServer server = new TcpServer();
            server.start(port, wsUrl);
        }
    }
}