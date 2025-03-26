package xzzzz.xz.echodb.backend.vm;


import xzzzz.xz.echodb.commen.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 在基于 2PL（两段锁协议） 的并发控制中，当一个事务（例如Tj）想要获取某个数据项的锁时，如果该锁已经被其他事务（例如Ti）持有，则Tj会被阻塞，直到Ti释放了该锁。
 * 这种等待关系可以被抽象成有向边，比如Tj在等待Ti，可以表示为Tj → Ti。
 * 通过记录所有事务之间的等待关系，就可以构建一个有向图，即等待图（Wait-for graph）。
 * 在等待图中，如果存在环路，即存在一个事务的等待序列形成了一个闭环，那么就说明存在死锁。
 * 因此，检测死锁只需要查看等待图中是否存在环即可。
 * <p>
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {

    /**
     * xid -> 持有的 uid 资源列表
     */
    private final Map<Long, List<Long>> x2u;

    /**
     * uid -> 当前正在被 xid 占用
     */
    private final Map<Long, Long> u2x;

    /**
     * uid -> 正在等待该资源的 xid 列表
     */
    private final Map<Long, List<Long>> wait;

    /**
     * 正在等待资源的 xid -> xid 的锁
     */
    private final Map<Long, Lock> waitLock;

    /**
     * xid -> 等待的资源 uid
     */
    private final Map<Long, Long> waitU;

    private final Lock lock;

    /**
     * xid -> “访问时间戳”，用于标记访问状态
     */
    private Map<Long, Integer> xidStamp;

    /**
     * “时间戳”策略标记每一次 DFS 的独立搜索路径
     */
    private int stamp;

    public LockTable() {
        this.x2u = new HashMap<>();
        this.u2x = new HashMap<>();
        this.wait = new HashMap<>();
        this.waitLock = new HashMap<>();
        this.waitU = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    /**
     * 在每次出现等待的情况时，就尝试向图中增加一条边，并进行死锁检测。如果检测到死锁，就撤销这条边，不允许添加，并撤销该事务
     * 返回为当前事务创建的新锁
     * <p>
     * 现实中肯定是这样的：
     * 多个事务（线程）并发地试图获取同一个或不同的资源（uid）
     * 每个线程都调用 add(xid, uid)，向系统申请资源的占有权
     * <p>
     * 这个资源可能：
     * ✅ 空闲 → 立即占有；
     * ❌ 被别的事务占着 → 进入等待队列 → 阻塞自己等待唤醒
     * 这就形成了经典的并发资源竞争问题，必须靠锁和等待机制来协调。
     * <p>
     * 为什么 add() 要加一个全局锁 lock.lock()？
     * 因为多个线程同时调用 add()，操作的是共享结构（比如 u2x, wait, x2u），必须加锁保证原子性和一致性。
     * <p>
     * 为什么 add() 里还要构造 Lock l = new ReentrantLock(); l.lock(); 返回给别人？
     * 因为当前线程抢资源失败，需要“等着”，构造一个线程级别的等候锁，交给自己以后挂起用。
     * <p>
     * 想象你做的是图书馆借书系统：
     * 每本书是一个 uid
     * 每个读者是一个 xid
     * add(xid, uid) 就是一个读者尝试借一本书
     * 如果书被别人借走了（被 u2x 占了），你就只能排队等（wait）
     * 图书管理员负责调度大家借书顺序（死锁检测 + 等待锁）
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();  // 锁定全局锁
        try {
            // 检查xid是否已经拥有这个uid资源（已经拥有这条边）
            if (isInList(x2u, xid, uid)) return null;
            // 检查uid资源未被其他xid事务持有
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 如果uid资源已经被其他事务持有，将当前事务添加到等待列表中
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            // 如果存在死锁
            if (hashDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            // 如果不存在死锁，为当前事务创建一个新的锁，并锁定它（每个线程各自 new 的锁对象，所以只会阻塞自己，等别人 unlock，不会影响其它线程）
            Lock l = new ReentrantLock();
            // 并发等待控制机制里的一个经典技巧：让当前事务在尝试访问资源时主动阻塞自己，等别人通知我才继续往下执行
            // 现在有两个事务：
            // T1 拿到了资源 uid
            // T2 也想拿 uid，但发现已经被占用了 ⇒ 加入等待队列
            // 于是 T2 调用了 add(xid, uid)，系统会给它返回一个 Lock l，并让它 l.lock() 来“等着”。
            l.lock();
            waitLock.put(xid, l);  // Lock 成为线程间通信的桥梁
            return l;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 判断uid1是否在uid0的val列表中
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) return false;
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            Long e = i.next();
            if (e == uid1) return true;
        }
        return false;
    }

    /**
     * 将uid1添加到uid0的val列表中
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if (!listMap.containsKey(uid0))
            listMap.put(uid0, new ArrayList<>());
        listMap.get(uid0).add(0, uid1);
    }

    /**
     * 将uid1从uid0的val列表中删除
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if (list == null) return;
        Iterator<Long> i = list.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if (e == uid1) {
                i.remove();
                break;
            }
        }
        if (list.isEmpty())
            listMap.remove(uid0);
    }

    /**
     * dfs逻辑，用来在等待图中查找是否存在循环依赖（即当前事务间接等待自己）
     * <p>
     * xid -> 正在等待的 uid -> 当前正在被 xid 占用
     * <p>
     * eg：A → 等资源1 → 被B占用 → B在等资源2 → 被A占用 ⇒ 回到了A ⇒ 环 ⇒ 死锁
     */
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);  // 从xidStamp映射中获取当前事务ID的时间戳
        if (stp != null) {
            if (stp == stamp)  // 该 xid 当前访问标记已经是 stamp，说明它回到了起点（形成了环）
                return true;
            // 虽然 stp < stamp 的事务可能在别的环上，但不在当前路径上形成环，不构成当前事务的死锁链条，所以可以返回 false
            if (stp < stamp)
                return false;
        }
        xidStamp.put(xid, stamp);  // 给当前 xid 打上当前 stamp

        Long uid = waitU.get(xid);  // 查找它在等待哪个资源（uid）
        if (uid == null) return false;  // 资源ID不存在，表示当前事务ID不在等待任何资源
        Long x = u2x.get(uid);  // 看这个资源当前是被哪个 xid 持有
        assert x != null;  // 断言失败（即 x == null），那么程序会抛出 AssertionError 异常，并终止当前线程的执行（除非捕获了这个异常）。
        return dfs(x);
    }

    /**
     * 检查所有持有uid资源的xid，看是否存在死锁
     */
    private boolean hashDeadLock() {
        this.xidStamp = new HashMap<>();
        this.stamp = 1;
        for (long xid : x2u.keySet()) {  // 遍历所有已经获得资源的事务ID（xid）
            Integer stp = xidStamp.get(xid);
            if (stp != null && stp > 0)  // 这个事务已经被上一次 DFS 搜索访问过（已经被赋过 stamp 值），说明它不会形成环，跳过
                continue;
            stamp++;  // 开始新一轮 DFS，用新的时间戳来区分新旧访问路径
            if (dfs(xid))
                return true;  // 如果发现有环（也就是说，事务间存在循环等待关系），就说明有死锁，立刻返回 true
        }
        return false;  // 如果所有的事务ID都被检查过，并且没有发现死锁
    }

    /**
     * 当一个事务commit或者abort时，就会释放掉它自己持有的锁，并将自身从等待图中删除，然后自身持有的uid资源会分配给新的xid占用
     */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> list = x2u.get(xid);
            if (list != null) {
                while (!list.isEmpty()) {
                    Long uid = list.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);  // V remove(Object key)：如果 key 不存在，Map 不变，返回 null。
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待uid的队列中选择一个xid来占用uid
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> list = wait.get(uid);
        if (list == null || list.isEmpty()) return;
        while (!list.isEmpty()) {
            long xid = list.remove(0);  // 获取并移除队列中的第一个xid
            if (waitLock.containsKey(xid)) {
                u2x.put(uid, xid);
                Lock lock = waitLock.remove(xid);
                waitU.remove(xid);
                lock.unlock();  // 业务线程就获取到了锁，就可以继续执行了
                break;
            }
        }
        if (list.isEmpty()) wait.remove(uid);
    }
}
