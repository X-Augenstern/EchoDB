package xzzzz.xz.echodb.backend;

import org.apache.commons.cli.*;
import xzzzz.xz.echodb.backend.dm.DataManager;
import xzzzz.xz.echodb.backend.server.Server;
import xzzzz.xz.echodb.backend.tbm.TableManager;
import xzzzz.xz.echodb.backend.tm.TransactionManager;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.vm.VersionManager;
import xzzzz.xz.echodb.backend.vm.VersionManagerImpl;
import xzzzz.xz.echodb.commen.Error;

public class Launcher {

    /**
     * 服务器监听的端口号
     */
    public static final int port = 9999;

    public static final long KB = 1 << 10;

    public static final long MB = 1 << 20;  // 1 << 20 = 2^20 = 1,048,576（单位是字节，即 1MB）

    /**
     * 默认的内存大小，这里是64MB，用于数据管理器
     */
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

    /**
     * 解析命令行参数中的内存大小
     */
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

    /**
     * 创建新的数据库
     */
    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    /**
     * 启动已有的数据库
     */
    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }
}
