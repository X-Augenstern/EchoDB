package xzzzz.xz.echodb.backend.vm;

import xzzzz.xz.echodb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * VM 对一个事务的抽象
 */
public class Transaction {

    /**
     * 事务的ID
     */
    public long xid;

    /**
     * 事务的隔离级别（0：读已提交；1：可重复读）
     */
    public int level;

    /**
     * 事务的快照，用于存储活跃事务的xid
     */
    public Map<Long, Boolean> snapshot;

    /**
     * 事务执行过程中的错误
     */
    public Exception err;

    /**
     * 标志事务是否自动中止
     */
    public boolean autoAborted;

    /**
     * 创建新事务
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if (level != 0) {  // 如果隔离级别不为0，创建快照
            t.snapshot = new HashMap<>();
            for (Long x : active.keySet()) {  // 将活跃事务的xid添加到快照中
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    /**
     * 判断一个事务是否仍然活跃（尚未提交）
     * <p>
     * SUPER_XID：通常在数据库管理系统中，SUPER_XID 可能代表 一个特殊的、系统级的事务 ID，比如数据库引擎用于维护数据一致性或执行系统管理操作的特殊事务。它不属于普通事务的一部分，通常不会生成或依赖快照。
     * <p>
     * 快照（Snapshot）：在 多版本并发控制（MVCC） 的系统中，事务通过创建自己的数据 快照 来保证 读取一致性。也就是说，每个事务在开始时会看到一个数据的快照，并且所有在事务执行过程中发出的读请求都将从这个快照中读取数据。
     * <p>
     * SUPER_XID 是一个系统级事务，通常具有以下几种特点：
     * 系统操作：它可能是为了执行一些系统管理任务（比如数据库恢复、元数据更新等）而存在。这些操作不需要像普通事务那样依赖 MVCC 快照来读取数据。
     * 不会参与常规的数据读取：通常，超级事务可能用于管理操作，不直接涉及常规的数据库数据读取，因此不需要快照来保证数据的一致性。
     * 它是一个“全局”事务：与普通事务通过创建自己的视图来读取数据不同，超级事务的读写不依赖常规的事务快照，因为它可能要查看数据库的“当前状态”，而不是某个特定的时间点。
     */
    public boolean isSnapshot(long xid) {
        if (xid == TransactionManagerImpl.SUPER_XID)
            return false;
        return snapshot.containsKey(xid);
    }
}
