package xzzzz.xz.echodb.backend;

import org.apache.commons.cli.*;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.commen.Error;

public class Launcher {
    public static final int port = 9999;

    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;  // 1 << 20 = 2^20 = 1,048,576（单位是字节，即 1MB）
    public static final long DEFAULT_MEM = MB * 64;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();  // 定义支持的命令行参数选项
        options.addOption("create", true, "-create DBPath");
        options.addOption("open", true, "-open DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);  // 解析命令行输入（args）

        if (cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        if (cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        System.out.println("Usage: Launcher (open|create) DBPath");
    }


    private static long parseMem(String memStr) {
        if (memStr == null || memStr.isEmpty()) return DEFAULT_MEM;
        if (memStr.length() < 3) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 2);  // [,)
        long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
        switch (unit) {
            case "KB" -> {
                return memNum * KB;
            }
            case "MB" -> {
                return memNum * MB;
            }
            case "GB" -> {
                return memNum * GB;
            }
            default -> Panic.panic(Error.InvalidMemException);
        }
        return DEFAULT_MEM;
    }

    private static void createDB(String path) {

    }

    private static void openDB(String path, long mem) {
    }
}
