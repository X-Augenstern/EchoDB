package xzzzz.xz.echodb.backend.dm.dataItem;

import xzzzz.xz.echodb.backend.common.SubArray;
import xzzzz.xz.echodb.backend.dm.DataManagerImpl;
import xzzzz.xz.echodb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataItem 是一个数据抽象层，它提供了一种在上层模块和底层数据存储之间进行交互的接口。其功能和作用主要包括：
 * <p>
 * 1. 数据存储和访问：DataItem 存储了数据的具体内容，以及一些相关的元数据信息，如数据的大小、有效标志等。上层模块可以通过 DataItem 对象获取到其中的数据内容，以进行读取、修改或删除等操作。
 * <p>
 * 2. 数据修改和事务管理：DataItem 提供了一些方法来支持数据的修改操作，并在修改操作前后执行一系列的流程，如保存原始数据、落日志等。这些流程保证了数据修改的原子性和一致性，同时支持事务管理，确保了数据的安全性。
 * <p>
 * 3. 数据共享和内存管理：DataItem 的数据内容通过 SubArray 对象返回给上层模块，这使得上层模块可以直接访问数据内容而无需进行拷贝。这种数据共享的方式提高了数据的访问效率，同时减少了内存的开销。
 * <p>
 * 4. 缓存管理：DataItem 对象由底层的 DataManager 缓存管理，通过调用 release() 方法可以释放缓存中的 DataItem 对象，以便回收内存资源，提高系统的性能和效率。
 * <p>
 * DataItem 提供了一种高层次的数据抽象，隐藏了底层数据存储的细节，为上层模块提供了方便的数据访问和管理接口，同时保证了数据的安全性和一致性。
 * <p>
 * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
 * 在修改之前需要调用 before() 方法，
 * 想要撤销修改时，调用 unBefore() 方法，
 * 在修改完成后，调用 after() 方法。
 * 整个流程，主要是为了保存前相数据，并及时落日志。DM 会保证对 DataItem 的修改是原子性的。
 * <p>
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    /**
     * 原始数据
     */
    private SubArray raw;

    /**
     * 旧的原始数据，数组长度就是原始数据的长度
     */
    private byte[] oldRaw;

    private Lock rLock;

    private Lock wLock;

    /**
     * 保存一个 dm 的引用是因为其释放依赖于 dm 的释放（dm 同时实现了缓存接口，用于缓存 DataItem），以及修改数据时落日志
     */
    private DataManagerImpl dm;

    /**
     * 唯一标识符：记录数据在第几页 pgno 的 offset 位置处被插入
     */
    private long uid;

    /**
     * 页面对象
     */
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, DataManagerImpl dm, long uid, Page pg) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
        ReadWriteLock lock = new ReentrantReadWriteLock();  // 可重入读写锁
        this.rLock = lock.readLock();
        this.wLock = lock.writeLock();
    }

    /**
     * 校验 ValidFlag 是否合法
     */
    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page getPage() {
        return this.pg;
    }

    @Override
    public long getUid() {
        return this.uid;
    }

    @Override
    public byte[] getOldRaw() {
        return this.oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return this.raw;
    }
}
