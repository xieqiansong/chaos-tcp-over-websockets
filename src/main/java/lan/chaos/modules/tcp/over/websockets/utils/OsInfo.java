package lan.chaos.modules.tcp.over.websockets.utils;

public class OsInfo {
    private static final String OS = System.getProperty("os.name").toLowerCase();
    public static final boolean isWindows = OS.contains("windows");

}