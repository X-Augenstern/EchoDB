package xzzzz.xz.echodb.backend.tbm;

import xzzzz.xz.echodb.backend.dm.DataManager;
import xzzzz.xz.echodb.backend.parser.statement.*;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.backend.vm.VersionManager;
import xzzzz.xz.echodb.commen.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TableManager 已经是直接被最外层 Server 调用（EchoDB 是 C/S 结构），方法直接返回执行结果，比如错误信息或者可读的结果信息的字节数组。
 * <p>
 * TBM 使用链表的形式将多张表组织起来，每一张表都保存一个指向下一张表的 UID。
 * <p>
 * - 这些方法的实现相对简单，主要是调用（VM）相关的方法来完成数据库操作。
 * <p>
 * - 在创建新表时，采用了头插法，即每次创建表都将新表插入到链表的头部，由 getFirstTableUid() 获取。这意味着最新创建的表会成为链表的第一个元素。
 * 由于使用了头插法，每次创建表都会改变表链表的头部，因此需要更新 Booter 文件，以便记录新的头表的UID。
 * <p>
 * - 在创建TBM对象时，会初始化表信息
 */
public class TableManagerImpl implements TableManager {

    /**
     * 版本管理器，用于管理事务的版本
     */
    VersionManager vm;

    /**
     * 数据管理器，用于管理数据的存储和读取
     */
    DataManager dm;

    /**
     * 启动信息管理器，用于管理数据库启动信息
     */
    private Booter booter;

    /**
     * 表缓存，用于缓存已加载的表，键是表名，值是表对象
     */
    private Map<String, Table> tableCache;

    /**
     * 事务表缓存，用于缓存每个事务修改过的表，键是事务ID，值是表对象列表
     */
    private Map<Long, List<Table>> xidTableCache;

    /**
     * 锁，用于同步多线程操作
     */
    private Lock lock;

    public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        this.lock = new ReentrantLock();
        loadTables();
    }

    /**
     * 依次加载所有的数据库表
     */
    private void loadTables() {
        long uid = getFirstTableUid();
        while (uid != 0) {  // 当UID不为0时，表示还有表需要加载
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);  // 将加载的表添加到表缓存中
        }
    }

    /**
     * 获取 Booter 文件的前8位字节，前8位存储第一张表的uid
     */
    private long getFirstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    /**
     * 将新的第一张表的uid写入 Booter 文件
     */
    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            List<Table> t = xidTableCache.get(xid);
            if (t == null)
                return "\n".getBytes();  // 没有事务相关的表，直接返回换行

            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if (tableCache.containsKey(create.tableName))
                throw Error.DuplicatedTableException;

            Table tb = Table.createTable(this, getFirstTableUid(), xid, create);  // 创建新的表，并获取表的UID
            updateFirstTableUid(tb.uid);
            tableCache.put(create.tableName, tb);  // 将新创建的表添加到表缓存中
            if (!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(tb);  // 将新创建的表添加到当前事务的表列表中
            return ("create" + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table tb = tableCache.get(insert.tableName);
        lock.unlock();
        if (tb == null)
            throw Error.TableNotFoundException;
        tb.insert(xid, insert);
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long xid, Select select) throws Exception {
        lock.lock();
        Table tb = tableCache.get(select.tableName);
        lock.unlock();
        if (tb == null)
            throw Error.TableNotFoundException;
        return tb.read(xid, select).getBytes();
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table tb = tableCache.get(update.tableName);
        lock.unlock();
        if (tb == null)
            throw Error.TableNotFoundException;
        int count = tb.update(xid, update);
        return ("update " + count).getBytes();
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table tb = tableCache.get(delete.tableName);
        lock.unlock();
        if (tb == null)
            throw Error.TableNotFoundException;
        int count = tb.delete(xid, delete);
        return ("delete " + count).getBytes();
    }
}
