package xzzzz.xz.echodb.backend.dm.page;

import xzzzz.xz.echodb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 存储在内存中的数据单元
 */
public class PageImpl implements Page {

    /**
     * 页面的页号，从1开始计数
     */
    private int pageNumber;

    /**
     * 实际包含的字节数据
     */
    private byte[] data;

    /**
     * 标志着页面是否是脏页面，在缓存驱逐时，脏页面需要被写回磁盘
     * <p>
     * 什么是脏页（Dirty Page）？
     * 在数据库或操作系统中，内存中的一页数据（比如你从磁盘读上来的数据库记录、文件数据）：
     * 被修改了（如你更新了一条记录），但是还没有同步写回磁盘，那么这页数据就被标记为 “脏的（dirty）”
     * <p>
     * 缓存驱逐时会发生什么？
     * 当内存不够，需要驱逐（Evict）某些页出去时：
     * 如果这个页是干净的（clean） 👉 可以直接丢掉，因为磁盘上那份是最新的 ✅
     * 如果这个页是脏的（dirty） 👉 必须先写回磁盘，否则你丢掉内存这一份，磁盘上就是老数据 ❌
     * <p>
     * 如果不写回脏页会发生什么？
     * 数据丢失！
     * 比如更新了某个数据库记录，但还没写回磁盘，页面被驱逐掉了，这条更新就永远消失了
     * 一旦系统崩溃、电源断电，磁盘上还是旧数据
     * <p>
     * 脏页机制的好处：
     * ✅ 避免频繁写磁盘（每次修改都立即写磁盘会很慢）
     * ✅ 利用内存做“写缓冲区”
     * ✅ 和“事务”、“日志”机制配合可以实现高性能又安全的数据管理
     */
    private boolean dirty;

    /**
     * 用于页面的锁
     */
    private Lock lock;

    /**
     * 保存了一个 PageCache 的引用，方便在拿到 Page 的引用时可以快速对页面的缓存进行释放操作
     */
    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return this.dirty;
    }

    @Override
    public int getPageNumber() {
        return this.pageNumber;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }
}
