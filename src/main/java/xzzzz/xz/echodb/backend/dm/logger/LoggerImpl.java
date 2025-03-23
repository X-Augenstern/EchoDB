package xzzzz.xz.echodb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.commen.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志读写
 * <p>
 * 日志的二进制文件，按照如下的格式进行排布：
 * [XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
 * XChecksum：一个四字节的整数，是对后续所有日志计算的校验和
 * Log1 ~ LogN：常规的日志数据
 * BadTail：在数据库崩溃时，没有来得及写完的日志数据（不一定存在）
 * <p>
 * 每条日志的格式如下：
 * Size：一个四字节整数，标识了 Data 段的字节数
 * Checksum：该条日志的校验和
 * [Size][Checksum][Data]
 * [0, 0, 0, 3] [3, -112, -4, 93] [97, 97, 97]
 * [0, 0, 0, 3] [14, 40, -23, -38] [98, 98, 98]
 * [0, 0, 0, 3] [24, -64, -41, 87] [99, 99, 99]
 * <p>
 * [Data] 的格式如下：
 * Update：[LogType](1) [XID](8) [UID](8) [OldRaw] [NewRaw]
 * Insert：[LogType](1) [XID](8) [Pgno](4) [Offset](2) [Raw]
 * <p>
 * Logger 被实现成迭代器模式，通过 next() 方法，不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
 * next() 方法的实现主要依靠 internNext();
 */
public class LoggerImpl implements Logger {

    private final static int SEED = 13331;

    /**
     * 每条日志的 [Size] 段偏移量
     */
    private final static int OF_SIZE = 0;

    /**
     * 每条日志的 [CheckSum] 段偏移量
     */
    private final static int OF_CHECKSUM = OF_SIZE + 4;

    /**
     * 每条日志的 [Data] 段偏移量 8
     */
    private final static int OF_DATA = OF_CHECKSUM + 4;

    public final static String LOG_SUFFIX = ".log";

    private RandomAccessFile file;

    private FileChannel fc;

    private Lock lock;

    /**
     * 当前日志指针的位置
     */
    private long position;

    /**
     * 打开时记录，log操作不更新
     */
    private long fileSize;

    /**
     * 四字节的整数，是对后续所有日志计算的校验和
     */
    private int xChecksum;

    public LoggerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        this.lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile file, FileChannel fc, int xChecksum) {
        this.file = file;
        this.fc = fc;
        this.xChecksum = xChecksum;
        this.lock = new ReentrantLock();
    }

    /**
     * 日志文件打开时的初始化，读取日志文件的 [XChecksum] 以及去除 [BadTail]
     */
    public void init() {
        long size = 0;
        try {
            size = this.file.length();  // 读取文件大小
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4)  // 若文件大小小于4，证明日志文件创建出现问题,XChecksum为4字节
            Panic.panic(Error.BadLogFileException);

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xCheckSum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xCheckSum;

        checkAndRemoveTail();
    }

    /**
     * [BadTail] 该条日志尚未写入完成，文件的校验和也就不会包含该日志的校验和，去掉 [BadTail] 即可保证日志文件的一致性
     */
    private void checkAndRemoveTail() {
        rewind();  // 将当前指针位置重置为常规日志开始的位置

        int xCheck = 0;  // 初始化校验和为 0
        while (true) {  // 循环读取日志，直到没有更多的日志可以读取
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calCheckNum(xCheck, log);  // 对所有日志计算的校验和
        }
        if (xCheck != xChecksum)  // 比较计算得到的校验和文件中的校验和，如果不相等，说明日志已经被破坏，抛出异常
            Panic.panic(Error.BadLogFileException);

        // 尝试将文件截断到当前位置，移除 [BadTail]
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        /*
            RandomAccessFile raf = new RandomAccessFile("test.txt", "rw");
            FileChannel fc = raf.getChannel();

            raf.seek(100);
            System.out.println(fc.position()); // 输出：100 ✅

            fc.position(200);
            raf.writeInt(42); // 会从 200 开始写 ✅

                     [文件]
                       ↑
                 ┌────────────┐
                 │ FileChannel│ <─── position()
                 └────────────┘
                       ↑
                 ┌────────────┐
                 │RandomAccess│ <─── seek()
                 └────────────┘
            RandomAccessFile.seek() 和 FileChannel.position() 操作的是同一个底层文件位置指针（file pointer），本质上就是“一个东西，两种 API 封装”

            控制器	            操作方式	                                感觉像是...
            RandomAccessFile	面向 stream 流的读写（面向流（RAF））	    用遥控器点歌
            FileChannel	        面向 buffer 的底层控制（面向缓冲区（NIO））	直接用 app 拖进度条
         */
        // 尝试把文件指针移动到 position 字节（最后的）位置
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }

        rewind();
    }

    /**
     * 读取下一条日志，如果出现异常则返回null
     */
    private byte[] internNext() {
        if (position + OF_DATA >= fileSize)  // [Size][Checksum]已经超过了文件大小
            return null;
        ByteBuffer tmp = ByteBuffer.allocate(4);  // 读取每条日志的Size：一个四字节整数，标识了 Data 段的字节数，进而得到Data段的字节数
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());  // 当前日志的Data段字节数
        if (position + size + OF_DATA > fileSize)  // [Size][Checksum][Data]已经超过了文件大小
            return null;

        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);  // 通过校验，开始读取当前日志
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        byte[] log = buf.array();
        int checkNum1 = calCheckNum(0, Arrays.copyOfRange(log, OF_DATA, log.length));  // 计算当前日志的校验和
        int checkNum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));  // 当前日志存储的校验和
        if (checkNum1 != checkNum2)
            return null;
        position += log.length;
        return log;
    }

    /**
     * 单条文件的校验和
     * 把每个字节按顺序组合进一个整数 xCheck 中，计算出一个 校验和，用于后续校验数据完整性
     * 在日志系统、数据库写入、数据传输中经常用来检测数据是否损坏或篡改
     */
    private int calCheckNum(int xCheck, byte[] log) {
        for (byte b : log)
            // 逐步混合进每个字节的值
            // 保证顺序敏感、内容敏感（只要内容或顺序变，校验值就变）
            xCheck = xCheck * SEED + b;  // 对 byte, short, char 进行数学运算时，它们会自动提升为 int，然后再参与计算
        return xCheck;
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());  // 把新的日志写入末尾
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    /**
     * 将数据解析成完整的log格式 [Size][Checksum][Data]
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checkSum = Parser.int2Byte(calCheckNum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checkSum, data);
    }

    /**
     * 更新总校验值
     */
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calCheckNum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(this.xChecksum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void truncate(long x) throws IOException {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
