package xzzzz.xz.echodb.backend.vm;

import xzzzz.xz.echodb.backend.common.AbstractCache;
import xzzzz.xz.echodb.backend.dm.DataManager;
import xzzzz.xz.echodb.backend.tm.TransactionManager;
import xzzzz.xz.echodb.backend.tm.TransactionManagerImpl;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.commen.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * VM 基于两段锁协议实现了调度序列的可串行化，并实现了 MVCC 以消除读写阻塞。同时实现了两种隔离级别。
 * <p>
 * 类似于 Data Manager 是 EchoDB 的数据管理核心，Version Manager 是 EchoDB 的事务和数据版本的管理核心。
 * <p>
 * MVCC 是怎么实现的？
 * 核心思想：一条数据存在多个版本，事务读取的是“自己视角下的数据版本”
 * <p>
 * 数据库会为每条数据维护：
 * xmin: 创建这条数据的事务ID
 * xmax: 删除这条数据的事务ID（如果没有被删就是 null）
 * <p>
 * 然后事务只会读取对自己“可见”的版本：
 * 没被其他事务插入/删除的版本
 * 没被自己之后的事务改动的版本
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;

    DataManager dm;

    /**
     * 目前正在活跃的事务映射，xid:对应的事务抽象
     */
    Map<Long, Transaction> activeTransaction;

    Lock lock;

    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();  // 获取锁，防止并发问题
        Transaction t = activeTransaction.get(xid);  // 从活动事务中获取事务对象
        lock.unlock();

        if (t.err != null)
            throw t.err;

        Entry entry;
        try {
            entry = super.get(uid);  // 尝试获取数据项
        } catch (Exception e) {
            if (e == Error.NullEntryException)
                return null;
            else
                throw e;
        }

        try {
            if (Visibility.isVisible(tm, t, entry))  // 如果数据项对当前事务可见，那么返回数据项的数据
                return entry.data();
            else
                return null;
        } finally {
            entry.release();
        }

    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.err != null)
            throw t.err;

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        // 全局互斥锁 lock，它不是“锁住某个变量”，而是锁住了这段代码执行的临界区，确保同一时间只有一个线程能执行这段逻辑，从而避免并发导致的数据冲突或状态不一致
        // 这个锁的行为是：
        // 全局的，只要多个线程都用的是这把锁，它们就会排队。
        // 不锁具体变量，而是锁住一段代码执行过程。
        // 它锁住了对 activeTransaction 这个共享 Map 的访问过程。
        // 虽然它没“绑定”在 activeTransaction 上，但它保护了访问这个变量的过程不会被多个线程同时执行。
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.err != null)
            throw t.err;

        Entry entry;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NullEntryException)  // 如果数据项不存在，那么返回false
                return false;
            else
                throw e;
        }

        try {
            if (!Visibility.isVisible(tm, t, entry))  // 基于 MVCC 的快照机制判断该事务是否有“读取权限”，如果数据项对当前事务不可见，那么返回false
                return false;
            Lock l;
            try {
                l = lt.add(xid, uid);
            } catch (Exception e) {
                t.err = Error.ConcurrentUpdateException;  // 如果出现并发更新的错误，那么中止事务，并抛出错误
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            /*
                操作方	            操作	            作用
                add()	            l.lock()	    锁上这把锁，让等待线程之后会阻塞
                调用者线程	        l.lock()	    在等待队列中被挂起，直到别人解锁
                selectNewXID()  	l.unlock()	    唤醒等待的线程
                调用者线程	        l.unlock()	    被唤醒后释放这把锁，别人才能继续用

                是不是“重复 lock”？
                不是的！ 是不同线程在 lock 同一把锁，目的是让当前线程阻塞在这儿，等待别人 unlock。
                add() 中的 l.lock() 是 制造阻塞条件
                delete() 中的 l.lock() 是 执行阻塞操作
                unlock 是由 资源释放方（别人） 调用的

                l.lock() 在访问/删除 uid 对应的数据前，必须先确保拿到了它的“访问权”，否则就可能引发并发冲突或数据不一致问题
                原因	                                解释
                保证并发事务不冲突	                    你不能和别人同时动一条数据
                实现串行化调度	                        一个事务访问时，其他人必须等
                符合事务隔离性（Isolation）	        防止版本跳跃、幻读、并发写冲突
                等到你能安全访问数据时再继续	            lock() 是在“排队等资源”
             */
            if (l != null) {
                l.lock();  // 如果这个资源已经被别人用着，那必须等别人释放再继续
                l.unlock();  // 拿到锁的目的只是“确认现在能动这个资源了”，一旦确认，后面的逻辑（版本判断、删除）可以继续
            }

            if (entry.getXmax() == xid)  // 如果数据项已经被当前事务删除，那么返回false
                return false;

            if (Visibility.isVersionSkip(tm, t, entry)) {  // 如果想删的记录中间有新版本插入 → 并发冲突 → 自动中止，并抛出错误
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);  // 将新的事务对象添加到活动事务的映射中
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);  // 从活动事务中获取事务对象
        lock.unlock();

        try {
            if (t.err != null) {
                throw t.err;
            }
        } catch (NullPointerException e) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(e);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if (!autoAborted)
            activeTransaction.remove(xid);  // 如果这不是一个自动中止的事务，那么从活动事务中移除这个事务
        lock.unlock();

        if (t.autoAborted) return;  // 如果事务已经被自动中止，那么直接返回，不做任何处理
        lt.remove(xid);
        tm.abort(xid);
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null)
            throw Error.NullEntryException;
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    /**
     * 在使用完 Entry 后，也应当及时调用 releaseEntry() 方法，释放掉 Entry 的缓存（由 VM 缓存 Entry）
     * 进而会由 DataItem 的引用释放掉 DataItem 的缓存（由 DM 缓存 DataItem）
     * 进而会释放包含的页面对象的缓存
     * <p>
     * 调用链：releaseForCache -> entry.remove -> dataItem.release
     */
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}
