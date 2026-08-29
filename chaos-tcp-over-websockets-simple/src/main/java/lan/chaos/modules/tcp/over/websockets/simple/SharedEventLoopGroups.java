package lan.chaos.modules.tcp.over.websockets.simple;

import cn.hutool.system.SystemUtil;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.NettyRuntime;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 进程级共享 EventLoopGroup：所有 Server/Client 复用同一对 boss/worker，
 * 避免每连接/每 server 各自 new NioEventLoopGroup 导致的线程爆炸与泄漏。
 * <p>
 * 用引用计数控制生命周期：第一次 acquire 时创建，release 归零时才真正 shutdownGracefully。
 */
@Slf4j
public final class SharedEventLoopGroups {
    private static final AtomicInteger refCount = new AtomicInteger(0);
    private static volatile EventLoopGroup bossGroup;
    private static volatile EventLoopGroup workerGroup;

    private SharedEventLoopGroups() {
    }

    private static EventLoopGroup create() {
        int nThreads = NettyRuntime.availableProcessors() * 2;
        return SystemUtil.getOsInfo().isWindows() ? new NioEventLoopGroup(nThreads) : new EpollEventLoopGroup(nThreads);
    }

    /**
     * 获取/创建共享 group，引用计数 +1。boss 固定 1 线程（仅 accept），worker 默认线程数（CPU×2）。
     */
    public static synchronized void acquire() {
        if (refCount.getAndIncrement() == 0) {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = create();
            log.info("共享 EventLoopGroup 已创建 (boss=1, worker=默认)");
        }
    }

    public static EventLoopGroup boss() {
        return bossGroup;
    }

    public static EventLoopGroup worker() {
        return workerGroup;
    }

    /**
     * 引用计数 -1，归零时关闭共享 group。多线程并发 close 安全。
     */
    public static synchronized void release() {
        if (refCount.decrementAndGet() <= 0) {
            refCount.set(0);
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
            bossGroup = null;
            workerGroup = null;
            log.info("共享 EventLoopGroup 已关闭");
        }
    }
}
