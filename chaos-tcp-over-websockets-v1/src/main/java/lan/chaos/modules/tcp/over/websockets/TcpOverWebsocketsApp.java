package lan.chaos.modules.tcp.over.websockets;

import lan.chaos.modules.tcp.over.websockets.launcher.TunnelLauncher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

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
        String mode = args.length > 0 ? args[0] : "";
        if (!"server".equals(mode) && !"client".equals(mode)) {
            printUsage();
            return;
        }
        SpringApplication app = new SpringApplication(TcpOverWebsocketsApp.class);
        app.setAdditionalProfiles(mode);
        app.run(args);
    }

    @Bean
    public ApplicationRunner run(TunnelLauncher launcher) {
        return args -> launcher.launch(args.getSourceArgs());
    }

    private static void printUsage() {
        System.out.println("example server:\n"
                + "    java -jar app.jar server 7002\n"
                + "example client:\n"
                + "    java -jar app.jar client 13306 ws://example.com:7002/forward/<target-host>/3306");
    }
}
