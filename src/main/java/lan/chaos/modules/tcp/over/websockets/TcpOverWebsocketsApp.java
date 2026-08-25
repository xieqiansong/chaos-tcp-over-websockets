package lan.chaos.modules.tcp.over.websockets;

import lan.chaos.modules.tcp.over.websockets.client.TcpServer;
import lan.chaos.modules.tcp.over.websockets.server.WebsocketServer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;

@Slf4j
@SpringBootApplication
public class TcpOverWebsocketsApp {

    /*
    example server (本地监听 7002, 供 client 连入):
        server 7002
    example client (本地监听 13306, 转发到远端 websocket 服务器及其背后的目标服务):
        client 13306 ws://example.com:7002/forward/<target-host>/3306
     */
    public static void main(String[] args) {
        SpringApplication.run(TcpOverWebsocketsApp.class, args);
    }

    @Bean
    public ApplicationRunner run() {
        return args -> {
            List<String> nonOptionArgs = Arrays.asList(args.getSourceArgs());
            if (nonOptionArgs.size() < 2) {
                printUsage();
                return;
            }
            String type = nonOptionArgs.get(0);
            int port;
            try {
                port = Integer.parseInt(nonOptionArgs.get(1));
            } catch (NumberFormatException e) {
                log.error("端口参数非法: {}", nonOptionArgs.get(1));
                printUsage();
                return;
            }
            if ("server".equals(type)) {
                @Cleanup
                WebsocketServer server = new WebsocketServer();
                server.start(port);
            } else if ("client".equals(type)) {
                if (nonOptionArgs.size() < 3) {
                    log.error("client 模式需要提供 wsUrl 参数");
                    printUsage();
                    return;
                }
                @Cleanup
                TcpServer server = new TcpServer();
                server.start(port, nonOptionArgs.get(2));
            } else {
                log.error("未知模式: {}, 仅支持 server / client", type);
                printUsage();
            }
        };
    }

    private static void printUsage() {
        System.out.println("example server:\n"
                + "    java -jar app.jar server 7002\n"
                + "example client:\n"
                + "    java -jar app.jar client 13306 ws://example.com:7002/forward/<target-host>/3306");
    }
}
