package lan.chaos.modules.tcp.over.websockets.simple.launcher;

import lan.chaos.modules.tcp.over.websockets.simple.server.WebsocketServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("server")
public class ServerLauncher implements TunnelLauncher {

    private final WebsocketServer websocketServer;

    public ServerLauncher(WebsocketServer websocketServer) {
        this.websocketServer = websocketServer;
    }

    @Override
    public void launch(String[] args) throws Exception {
        if (args.length < 2) {
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
        websocketServer.start(port);
    }

    private void printUsage() {
        System.out.println("example server:\n    java -jar app.jar server 7002");
    }
}
