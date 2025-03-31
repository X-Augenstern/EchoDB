package xzzzz.xz.echodb.backend.dm.pageCache;

import xzzzz.xz.echodb.backend.common.AbstractCache;
import xzzzz.xz.echodb.backend.dm.page.Page;
import xzzzz.xz.echodb.backend.dm.page.PageImpl;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.commen.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于引用计数策略的缓存框架
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;

    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;

    private FileChannel fc;

    private Lock fileLock;

    /**
     * 当前这个数据库文件中一共已经有多少页
     */
    private AtomicInteger pageNumbers;

    public PageCacheImpl(int maxResource, RandomAccessFile file, FileChannel fc) {
        super(maxResource);

        if (maxResource < MEM_MIN_LIM)  // 最大缓存资源数<内存最小限制
            Panic.panic(Error.MemTooSmallException);

        long length = 0;  // 当前数据库文件的总字节数
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    /**
     * 从文件中读取 pgno 页的页面数据，并将其包裹成 Page 返回
     */
    @Override
    protected Page getForCache(long key) {
        int pgno = (int) key;
        long offset = pageOffset(pgno);  // 根据页码计算偏移量

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);  // 分配一个一页大小的buffer
        fileLock.lock();
        try {
            fc.position(offset);  // 从文件通道位置读取数据至buffer
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 在驱逐页面时将脏页面写回磁盘，保证持久化
     */
    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();  // 新建页面时自增
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);  // 新建的页面需要立刻写回
        return pgno;
    }

    /**
     * 将某个 Page 的数据从内存写回文件的操作，确保数据被同步保存到磁盘中，保证持久化
     * Page 在内存中修改了之后，需要在合适的时机刷回磁盘
     * 写回操作使用锁来保证写入的原子性和线程安全性
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();  // 加锁，防止多个线程同时对文件写入，确保线程安全
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 根据页号计算对应的文件偏移量
     */
    private static long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;  // 页号从 1 开始
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get(pgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        super.release(page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);  // 截断点是 maxPgno+1 的文件偏移位置，保留页号 1-maxPgno
        try {
            file.setLength(size);  // 将文件的总长度设置为 size 字节，这会删除多余部分，保留到设定的页号
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);  // 截断后，最大的有效页号是 maxPgno，下一次新建页面时就会往后分配
    }

    @Override
    public int getTotalPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }
}
