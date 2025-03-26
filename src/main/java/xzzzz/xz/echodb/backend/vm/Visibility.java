package xzzzz.xz.echodb.backend.vm;

import xzzzz.xz.echodb.backend.tm.TransactionManager;

public class Visibility {

    /*
        读未提交（Read Uncommitted）
        在这种隔离级别下，事务可以读取其他事务尚未提交的数据。
        优点：性能较高，因为事务间几乎没有等待。
        缺点：可能导致脏读，即读取到其他事务未提交的、可能最终会被回滚的数据。

        读已提交（Read Committed）
        事务只能读取其他事务已经提交的数据。
        优点：避免了脏读问题。
        缺点：可能导致不可重复读，即在同一事务中多次读取同一数据，可能因为其他事务的修改而得到不同的结果。

        可重复读（Repeatable Read）
        在同一事务中多次读取同一数据时，能够保证读取到的数据是一致的。
        优点：避免了脏读和不可重复读问题。
        缺点：在特定情况下可能导致幻读，即同一事务内连续执行两次相同的查询，第二次查询可能会返回第一次查询没有的新行（因为其他事务插入了新数据）。

        序列化（Serializable）
        最高的隔离级别，事务串行执行，避免了脏读、不可重复读和幻读问题。
        优点：完全隔离，保证数据一致性。
        缺点：性能最低，因为可能导致大量的锁竞争和事务等待。
     */

    /**
     * 读已提交隔离级别下，某个事务是否可以读取指定数据（Entry e）的内容
     * <p>
     * 创建事务自己修改并且没有删除：数据由当前事务创建，并且没有删除操作，事务可以读取。
     * 创建事务已经提交并且没有删除：只要数据项的创建事务已经提交，且没有删除操作，数据是可以安全读取的。
     * 有删除事务但删除未提交：删除事务未提交时，数据仍然有效，当前事务可以读取。
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();  // 获取记录的创建版本号xid
        long xmax = e.getXmax();  // 获取记录的删除版本号

        // 既然数据是当前事务创建的，并且没有删除操作，那么当前事务完全可以读取自己已经修改的数据项。没有其他事务的修改或者删除干扰。
        if (xmin == xid && xmax == 0) return true;

        // 检查数据项的创建事务（xmin）是否已经提交。如果创建事务已经提交，说明这个数据项是合法的、可见的，意味着它对数据的修改已经生效。
        if (tm.isCommitted(xmin)) {
            // 如果数据项没有被删除，说明它仍然存在于数据库中。即使是其他事务修改了数据，只要当前事务看到的是提交过的数据，数据就是安全且可读的。
            if (xmax == 0) return true;
            // 即使某个事务标记了删除操作，如果删除事务还未提交，则说明删除操作是“暂时的”，并不具有最终性。此时，读取操作还是安全的，因为这个数据项在事务隔离下还保持有效，尚未删除。
            if (xmax != xid) {
                // 如果 xmax == xid，这意味着当前事务删除了这条数据。
                // 如果当前事务在删除数据的同时又想读取它，这就形成了一个逻辑矛盾。不应该读取自己标记删除的数据。删除操作本身意味着数据已经不再有效，读取它是违反常理的。
                // 因为删除操作已经标记了数据“已废弃”，读取时应该返回无效或已删除的数据，这就不符合 Read Committed 隔离级别的规则。
                // 正常情况下，删除操作是由其他事务执行的。
                // 如果 xmax != xid，意味着删除操作是由其他事务完成的。
                // 在这种情况下，如果删除事务 未提交，那么当前事务依然可以读取该数据项，因为它还没有被完全删除（“暂时删除”）。
                // 如果删除事务 已提交，则当前事务应该看到数据已经被删除，不能读取这个数据。
                return !tm.isCommitted(xmax);  // 记录的删除版本未提交。因为没有提交，代表该数据还是上一个版本可见的
            }
        }
        return false;
    }

    /**
     * 可重复读隔离级别下，某个事务是否可以读取指定数据（Entry e）的内容
     * <p>
     * 在 Repeatable Read 隔离级别下：
     * 同一个事务在多次读取相同数据时，应该得到相同的结果，即该数据在事务开始时的快照应该保持不变。
     * 在这种隔离级别下，数据不应该受到其他事务的修改或删除影响。
     * <p>
     * 读提交会导致不可重复读和幻读。这里我们来解决不可重复读的问题。
     * T1 begin
     * R1(X) // T1 读得 0
     * T2 begin
     * U2(X) // 将 X 修改为 1
     * T2 commit
     * R1(X) // T1 读的 1
     * 可以看到，T1 两次读 X，读到的结果不一样。如果想要避免这个情况，就需要引入更严格的隔离级别，即可重复读（repeatable read）。
     * T1 在第二次读取的时候，读到了已经提交的 T2 修改的值，导致了这个问题。
     * <p>
     * 于是我们可以规定：事务只能读取它开始时, 就已经结束的那些事务产生的数据版本
     * 这条规定，增加于，事务需要忽略：
     * 在本事务后开始的事务的数据;
     * 本事务开始时还是 active 状态的事务的数据
     * 对于第一条，只需要比较事务 xid，即可确定。
     * 而对于第二条，则需要在事务 Ti 开始时，记录下当前活跃的所有事务 SP(Ti)，如果记录的某个版本，XMIN 在 SP(Ti) 中，也应当对 Ti 不可见。
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        if (xmin == xid && xmax == 0) return true;

        // 由一个已提交的事务创建且这个事务小于Ti且这个事务在Ti开始前提交
        if (tm.isCommitted(xmin) && xmin < xid && !t.isSnapshot(xmin)) {
            if (xmax == 0) return true;
            if (xmax != xid) {
                // 这个事务尚未提交或这个事务在Ti开始之后才开始或这个事务在Ti开始前还未提交
                // 1.如果事务id为 xmax 的事务还没有提交，说明该数据对当前事务是可见的
                // 2.如果事务id xmax > xid，说明修改数据的事务在当前事务后发生，因此该数据对当前事务可见
                // 3.如果 xmax 在正在运行的事务快照中，说明本事务开始时 xmax 还是 active 状态的事务，因此忽略 xmax 对数据的修改，读取之前版本的数据
                return !tm.isCommitted(xmax) || xmax > xid || t.isSnapshot(xmax);
            }
        }
        return false;
    }

    /**
     * 版本跳跃的检查，取出要修改的数据 X 的最新提交版本，并检查该最新版本的创建者对当前事务是否可见
     * <p>
     * MVCC 的实现，使得 EchoDB 在撤销或是回滚事务很简单：
     * 只需要将这个事务标记为 aborted 即可。
     * 根据可见性，每个事务都只能看到其他 committed 的事务所产生的数据，一个 aborted 事务产生的数据，就不会对其他事务产生任何影响了，也就相当于，这个事务不曾存在过。
     * <p>
     * 版本跳跃问题，考虑如下的情况，假设 X 最初只有 x0 版本，T1 和 T2 都是可重复读的隔离级别：
     * T1 begin
     * T2 begin
     * R1(X) // T1读取x0
     * R2(X) // T2读取x0
     * U1(X) // T1将X更新到x1
     * T1 commit
     * U2(X) // T2将X更新到x2
     * T2 commit
     * 这种情况实际运行起来是没问题的，但是逻辑上不太正确。T1 将 X 从 x0 更新为了 x1，这是没错的。但是 T2 则是将 X 从 x0 更新成了 x2，跳过了 x1 版本。
     * 读提交是允许版本跳跃的，而可重复读则是不允许版本跳跃的。
     * <p>
     * 版本跳跃问题是指在多版本并发控制（MVCC）中，一个事务要修改某个数据项时，可能会出现跳过中间版本直接修改最新版本的情况，从而产生逻辑上的错误。
     * 解决版本跳跃的关键在于检查最新版本的创建者对当前事务是否可见。如果当前事务要修改的数据已经被另一个事务修改并且对当前事务不可见，就要求当前事务回滚。
     * 具体来说，对于事务Ti要修改数据X的情况下，要检查如下两种情况：
     * <p>
     * 1. 如果另一个事务Tj的事务ID（XID）大于Ti的事务ID，则Tj在时间上晚于Ti开始，因此Ti应该回滚，避免版本跳跃。
     * 2. 如果Tj在Ti的快照集合（SP(Ti)）中，则Tj在Ti开始之前已经提交，但Ti在开始之前并不能看到Tj的修改，因此也应该回滚。
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        // 如果删除事务已提交，且删除事务发生在当前事务之后，或者当前事务已经为删除事务创建了快照，则跳过该版本
        return t.level == 0 ? false : tm.isCommitted(xmax) && (xmax > t.xid || t.isSnapshot(xmax));
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        return t.level == 0 ? readCommitted(tm, t, e) : repeatableRead(tm, t, e);
    }
}
