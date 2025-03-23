package xzzzz.xz.echodb.backend.dm.logger;

import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.commen.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

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

    enum Mode {CREATE, OPEN}

    /**
     * 封装 fc + raf 的结构体
     */
    class FileInfo {
        private final FileChannel fc;

        private final RandomAccessFile raf;

        public FileInfo(FileChannel fc, RandomAccessFile raf) {
            this.fc = fc;
            this.raf = raf;
        }

        public FileChannel getFc() {
            return this.fc;
        }

        public RandomAccessFile getRaf() {
            return this.raf;
        }
    }

    /**
     * 日志文件的创建，初始化 [XChecksum] 为 0
     */
    static Logger create(String path) {
        FileInfo fi = access(path, Mode.CREATE);
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
        FileInfo fi = access(path, Mode.OPEN);
        LoggerImpl lg = new LoggerImpl(fi.getRaf(), fi.getFc());
        lg.init();
        return lg;
    }

    private static FileInfo access(String path, Mode mode) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            if (mode == Mode.CREATE) {
                if (!f.createNewFile())
                    Panic.panic(Error.FileExistsException);
            } else {
                if (!f.exists())
                    Panic.panic(Error.FileNotExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite())
            Panic.panic(Error.FileCannotRWException);

        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>(2);
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new FileInfo(fc, raf);
    }
}
