package xzzzz.xz.echodb.backend.tm;


/**
 * TM通过维护XID文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态
 */
public interface TransactionManager {
    /**
     * 开启一个新事务
     */
    long begin();

    /**
     * 提交一个新事务
     */
    void commit(long xid);

    /**
     * 取消一个新事务
     */
    void abort(long xid);

    /**
     * 查询事务状态是否正在进行
     */
    boolean isActive(long xid);

    /**
     * 查询事务状态是否已提交
     */
    boolean isCommitted(long xid);

    /**
     * 查询事务状态是否已取消（回滚）
     */
    boolean isAborted(long xid);

    /**
     * 关闭TM
     */
    void close();
}
