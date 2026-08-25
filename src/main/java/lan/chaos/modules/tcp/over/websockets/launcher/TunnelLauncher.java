package lan.chaos.modules.tcp.over.websockets.launcher;

/**
 * 隧道启动器抽象。server / client 两种模式各有一个 {@code @Profile} 实现，
 * 由 {@link TcpOverWebsocketsApp} 注入后按命令行参数启动对应隧道。
 */
public interface TunnelLauncher {
    void launch(String[] args) throws Exception;
}
