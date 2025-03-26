package xzzzz.xz.echodb.backend.vm;

import xzzzz.xz.echodb.backend.dm.DataManager;
import xzzzz.xz.echodb.backend.tm.TransactionManager;

public interface VersionManager {

    /**
     * 读取 entry 的 [Data] 部分
     */
    byte[] read(long xid, long uid) throws Exception;

    /**
     * 将数据包裹成 Entry，然后交给 DM 插入即可
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * 删除一个数据项
     */
    boolean delete(long xid, long uid) throws Exception;

    /**
     * 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用
     */
    long begin(int level);

    /**
     * 提交一个事务，主要就是释放掉相关的结构，并且释放持有的锁，并修改 TM 状态
     */
    void commit(long xid) throws Exception;

    /**
     * abort 事务的方法有两种：手动和自动。
     * 手动指的是调用 abort() 方法，而自动，则是在事务被检测出现死锁时，会自动撤销来回滚事务；或者出现版本跳跃时，也会自动回滚
     */
    void abort(long xid);

    static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
