package xzzzz.xz.echodb.backend.dm.logger;

import xzzzz.xz.echodb.backend.utils.FileInfo;
import xzzzz.xz.echodb.backend.utils.FileUtil;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.utils.Parser;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {

    /**
     * 向日志文件写入日志
     */
    void log(byte[] data);

    /**
     * 将文件截断到当前位置
     */
    void truncate(long x) throws IOException;

    /**
     * 读取下一条日志的 [Data]
     */
    byte[] next();

    /**
     * 日志指针置为4
     */
    void rewind();  // 重绕，倒带

    /**
     * 关闭日志
     */
    void close();

    /**
     * 日志文件的创建，初始化 [XChecksum] 为 0
     */
    static Logger create(String path) {
        FileInfo fi = access(path, FileUtil.Mode.CREATE);
        FileChannel fc = fi.getFc();

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));  // 将0转换成四字节的数字
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new LoggerImpl(fi.getRaf(), fc, 0);
    }

    /**
     * 打开日志文件，需要首先校验日志文件的 [XChecksum]，并移除文件尾部可能存在的 [BadTail]
     */
    static Logger open(String path) {
        FileInfo fi = access(path, FileUtil.Mode.OPEN);
        LoggerImpl lg = new LoggerImpl(fi.getRaf(), fi.getFc());
        lg.init();
        return lg;
    }

    private static FileInfo access(String path, FileUtil.Mode mode) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        return FileUtil.checkFileAndBuildInfo(f, mode);
    }
}
