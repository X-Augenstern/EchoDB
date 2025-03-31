package xzzzz.xz.echodb.backend.tbm;

import xzzzz.xz.echodb.backend.utils.FileUtil;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.commen.Error;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * 启动信息管理
 * <p>
 * - MYDB的启动信息存储在 bt 文件中，所需的信息只有一个，即头表的UID。
 * <p>
 * - Booter 类提供了 load 和 update 两个方法，用于加载和更新启动信息。
 * <p>
 * - update 方法在修改 bt 文件内容时，采取了一种保证原子性的策略，即先将内容写入一个临时文件 bt_tmp 中，然后通过操作系统的重命名操作将临时文件重命名为 bt 文件。
 * <p>
 * - 通过这种方式，利用操作系统重命名文件的原子性，来确保对 bt 文件的修改操作是原子的，从而保证了启动信息的一致性和正确性。
 */
public class Booter {

    /**
     * 数据库启动信息文件的后缀
     */
    public final static String BOOTER_SUFFIX = ".bt";

    /**
     * 数据库启动信息文件的临时后缀
     */
    public final static String BOOTER_TMP_SUFFIX = ".bt_tmp";

    /**
     * 无后缀的数据库启动信息文件的路径
     */
    String path;

    /**
     * 数据库启动信息文件
     */
    File file;

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    /**
     * 删除指定路径上可能存在的临时文件
     */
    private static void removeBadTmp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    /**
     * 在指定路径新建 .bt 文件
     */
    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        FileUtil.checkFile(f, FileUtil.Mode.CREATE);
        return new Booter(path, f);
    }

    /**
     * 打开指定路径 .bt 文件
     */
    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        FileUtil.checkFile(f, FileUtil.Mode.OPEN);
        return new Booter(path, f);
    }

    /**
     * 加载文件启动信息文件。从文件中读取全部内容，并返回一个 byte[]（字节数组）
     */
    public byte[] load() {
        byte[] buf = null;
        try {
            // Files.readAllBytes(file.toPath())：Java NIO 中的一个便捷方法 -> 将整个文件的内容一次性读入内存并返回一个字节数组（byte[]）
            // file.toPath()：将 File 对象转换为 Path，这是 Files 方法要求的参数
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /**
     * 更新启动信息文件的内容：创建临时文件 -> 写入临时文件 -> 临时文件替代原文件（原子性） -> 更新原文件
     */
    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();  // 尝试创建新的临时文件
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!tmp.canRead() || !tmp.canWrite())
            Panic.panic(Error.FileCannotRWException);

        // "try-with-resources"（带资源的 try 语句）
        // 自动关闭在括号里声明的资源（如文件流、数据库连接、网络流等），不需要手动 close()
        // 即使发生异常，或者代码正常结束，out.close() 都会自动调用！
        // 多资源写法：
        // try (
        //    FileInputStream in = new FileInputStream("input.txt");
        //    FileOutputStream out = new FileOutputStream("output.txt")
        //)
        // 括号里的对象必须实现 AutoCloseable 接口
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);  // 将数据写入临时文件
            out.flush();  // 刷新输出流，确保数据被写入文件
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            // 将临时文件移动到启动信息文件的位置，替换原来的文件
            Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }

        file = new File(path + BOOTER_SUFFIX);  // 更新file字段为新的启动信息文件
        if (!file.canRead() || !file.canWrite())
            Panic.panic(Error.FileCannotRWException);
    }
}
