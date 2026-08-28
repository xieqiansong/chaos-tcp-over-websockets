package lan.chaos.modules.tcp.over.websockets.launcher;

import lan.chaos.modules.tcp.over.websockets.client.TcpServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("client")
public class ClientLauncher implements TunnelLauncher {

    private final TcpServer tcpServer;

    public ClientLauncher(TcpServer tcpServer) {
        this.tcpServer = tcpServer;
    }

    @Override
    public void launch(String[] args) throws Exception {
        if (args.length < 3) {
            log.error("client 模式需要参数: <port> <wsUrl>");
            printUsage();
            return;
        }
        int port;
        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            log.error("端口参数非法: {}", args[1]);
            printUsage();
            return;
        }
        tcpServer.start(port, args[2]);
    }

    private void printUsage() {
        System.out.println("example client:\n    java -jar app.jar client 13306 ws://example.com:7002/forward/<target-host>/3306");
    }
}
