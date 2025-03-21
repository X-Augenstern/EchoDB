package xzzzz.xz.echodb.backend.tm;

import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.commen.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {

    static final int LEN_XID_HEADER_LENGTH = 8;  // XID文件头长度，记录了这个 XID 文件管理的事务的个数
    private static final int XID_FIELD_SIZE = 1;  // 每个事务的占用长度
    static final String XID_SUFFIX = ".xid";  // XID 文件后缀


    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;
    public static final long SUPER_XID = 0;  // 超级事务，永远为committed状态

    private RandomAccessFile file;
    private FileChannel fc;  // 表示打开的某个文件
    private long xidCounter;
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查xid文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     * <p>
     * [ 文件起始处 ]
     * |-- XID Header (固定长度，比如8字节，存储 xidCounter) --|
     * |-- XID Entry 1 (比如1字节，表示事务1状态) --|
     * |-- XID Entry 2 (比如1字节，表示事务2状态) --|
     * |-- XID Entry 3 (比如1字节，表示事务3状态) --|
     * <p>
     * 验证事务文件的长度是否和 xidCounter 的值一致，确保没有缺失或多写事务状态字节
     * 这是一种非常典型的文件元数据校验方式，在数据库系统、自研存储引擎中很常见
     */
    private void checkXIDCounter() {
        long fileLen = 0;  // 初始化文件长度为0
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);  // 如果获取不到文件长度
        }
        if (fileLen < LEN_XID_HEADER_LENGTH)
            Panic.panic(Error.BadXIDFileException);  // 如果文件长度<XID头部长度

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);  // 将文件通道的位置设置为0
            // 从文件通道当前位置读取字节数据，尽量填充 ByteBuffer，但不保证一次就填满。返回值是实际读取的字节数（可能小于 buffer 剩余空间）
            // 只会尽可能读取可用的数据到 buf 中，读取多少取决于：
            // buf.remaining()（还有多少空间没写） | fc 当前 position() 后，文件还剩多少字节可读
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());  // 将ByteBuffer的内容解析为long，作为xidCounter
        long end = getXIDPosition(this.xidCounter + 1);  // 计算xidCounter+1对应的XID位置，也就是文件应该有的长度
        if (end != fileLen)
            Panic.panic(Error.BadXIDFileException);  // 如果计算出的XID位置与文件长度不符
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     */
    private long getXIDPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态为status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXIDPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);  // 设置当前写入位置为 offset
            fc.write(buf);  // 把 buf 的内容写入文件（此时只写了 tmp[0] 里那一个字节，更新事务的状态）
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制把文件通道中尚未写入磁盘的内容刷新（flush）到硬盘上，确保数据不丢失
            // 通常，fc.write(buf) 只是把数据写入操作系统的文件缓存（page cache）
            // 这意味着写操作可能还没真正写入物理磁盘，此时系统崩溃、断电或宕机会丢失刚写的数据
            // -> 把通道中的写入内容同步到磁盘（保证持久化）
            // false 表示只刷新数据本身，不强求刷新文件元数据（比如修改时间）
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            // 会从当前位置（position = 0）开始写入 ByteBuffer 中的内容，并覆盖原有数据，直到：
            // ByteBuffer 中的数据写完 | 文件剩余空间不够写完
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 检测XID事务是否处于status状态
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXIDPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
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
