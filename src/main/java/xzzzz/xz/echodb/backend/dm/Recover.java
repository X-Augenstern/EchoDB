package xzzzz.xz.echodb.backend.dm;

import com.google.common.primitives.Bytes;
import xzzzz.xz.echodb.backend.common.SubArray;
import xzzzz.xz.echodb.backend.dm.dataItem.DataItem;
import xzzzz.xz.echodb.backend.dm.logger.Logger;
import xzzzz.xz.echodb.backend.dm.page.Page;
import xzzzz.xz.echodb.backend.dm.page.PageX;
import xzzzz.xz.echodb.backend.dm.pageCache.PageCache;
import xzzzz.xz.echodb.backend.tm.TransactionManager;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.backend.utils.UidUtil;
import xzzzz.xz.echodb.commen.Error;

import java.util.*;

/**
 * DM 为上层模块提供了两种操作，分别是插入新数据（I）和更新现有数据（U），删除数据在 VM 中
 * <p>
 * 🛠 数据库恢复的哲学是：
 * 不信任任何中间状态，只信任日志。
 * 因为恢复阶段不能依赖值，要严格按日志走，确保一致性
 * <p>
 * 数据库恢复哲学的“金句”：“不管现在的值是什么，只看它恢复后该是什么。”
 * 这正是日志驱动恢复机制（Write-Ahead Logging, WAL）的核心理念！
 * “Repeating history during redo, undoing what never committed.”
 * 重演历史： 把所有日志中能做的操作都再做一遍（重做）
 * 撤销未完成： 把没提交的全都撤回，不留痕迹
 * <p>
 * 为什么只看日志、不看当前值？
 * 因为崩溃之后，磁盘里的数据状态是不可预知的，可能是：
 * （1）事务提交了，但数据没写进去（No Force）
 * （2）事务没提交，但数据写进去了（Steal）
 * （3）数据页写了一半
 * （4）缓存页未同步
 * （5）崩溃发生在某个中间点
 * <p>
 * 💥 所以数据库必须 “对当前值失去信任”，只相信日志里记下的：
 * 事务做了什么（undo）
 * 事务完成了什么（redo）
 * <p>
 * 数据库恢复的行为准则：
 * 操作类型	    日志信息	        恢复操作
 * 未提交事务	    (前值, 后值)	    撤销（undo）→ 恢复成前值
 * 已提交事务	    (前值, 后值)	    重做（redo）→ 强制写后值
 * 无论磁盘上的值是啥：
 * 撤销就是改回去（即使值已经是“前值”了，仍会尝试）
 * 重做就是写进去（即使已经是“后值”了，也会再写一遍）
 * <p>
 * DM 的日志策略：在进行 I 和 U 操作之前，必须先进行对应的日志操作，在保证日志写入磁盘后，才进行数据操作
 * 这个日志策略，使得 DM 对于数据操作的磁盘同步，可以更加随意。
 * 日志在数据操作之前，保证到达了磁盘，那么即使该数据操作最后没有来得及同步到磁盘，数据库就发生了崩溃，后续也可以通过磁盘上的日志恢复该数据
 * <p>
 * 对于两种数据操作，DM 记录的日志如下：
 * (Ti, I, A, x)，表示事务 Ti 在 A 位置插入了一条数据 x
 * (Ti, U, A, oldx, newx)，表示事务 Ti 将 A 位置的数据，将 oldx 更新成 newx
 * <p>
 * 在 EchoDB 中，有两条规则限制了数据库的操作，以便于恢复日志：
 * 1、正在进行的事务，不会读取其他任何未提交的事务产生的数据
 * 2、正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据
 * <p>
 * 根据上方的两条规则，日志的恢复也分为两种：
 * 1. 通过 redo log 重做所有崩溃时已经完成（committed 或 aborted）的事务
 * 2. 通过 undo log 撤销所有崩溃时未完成（active）的事务
 * 在恢复后，数据库就会恢复到所有已完成事务结束，所有未完成事务尚未开始的状态
 * <p>
 * redo：
 * 1. 正序扫描事务 T 的所有日志
 * 2. 如果日志是插入操作 (Ti, I, A, x)，就将 x 重新插入 A 位置
 * 3. 如果日志是更新操作 (Ti, U, A, oldx, newx)，就将 A 位置的值设置为 newx
 * <p>
 * undo：
 * 1. 倒序扫描事务 T 的所有日志
 * 2. 如果日志是插入操作 (Ti, I, A, x)，就将 A 位置的数据删除
 * 3. 如果日志是更新操作 (Ti, U, A, oldx, newx)，就将 A 位置的值设置为 oldx
 */
public class Recover {

    private final static byte LOG_TYPE_INSERT = 0;

    private final static byte LOG_TYPE_UPDATE = 1;

    private final static int REDO = 0;

    private final static int UNDO = 1;

    // [LogType](1B) [XID](8) [UID](8) [OldRaw] [NewRaw]
    private final static int OF_TYPE = 0;

    private final static int OF_XID = OF_TYPE + 1;

    private final static int OF_UPDATE_UID = OF_XID + 8;

    private final static int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    // [LogType](1) [XID](8) [Pgno](4) [Offset](2) [Raw]
    private final static int OF_INSERT_PGNO = OF_XID + 8;

    private final static int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;

    private final static int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    /**
     * Insert 日志信息：[LogType](1) [XID](8) [Pgno](4) [Offset](2) [Raw]
     */
    static class InsertLogInfo {
        long xid;  // 事务ID
        int pgno;  // 数据页
        short offset;  // 页内偏移
        byte[] raw;  // 插入的值
    }

    /**
     * 将 [Data] 解析为 Insert 日志信息
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    /**
     * update 日志信息：[LogType](1) [XID](8) [UID](8) [OldRaw] [NewRaw]
     * [UID] = [Pgno](4) (保留2) [Offset](2)
     * |------ 高 32 位 pgno ------|-- 中间 16 位（保留） --|-- 低 16 位 offset --|
     * |      页号（Page Number）  |        保留字段        |     页内偏移量       |
     * <p>
     * [OldRaw]、[NewRaw] 长度视为相等
     */
    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;  // 修改前的原始值
        byte[] newRaw;  // 修改后的新值
    }

    /**
     * 将 [Data] 解析为 Update 日志信息
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        UidUtil.UidInfo uidInfo = UidUtil.parseUid(uid);
        li.offset = uidInfo.getOffset();
        li.pgno = uidInfo.getPgno();
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + 2 * length);
        return li;
    }

    private static boolean isInertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /**
     * 重做所有已完成（提交/取消）的事务
     */
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                if (!tm.isActive(li.xid))
                    doInsertLog(pc, li, REDO);
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                if (!tm.isActive(li.xid))
                    doUpdateLog(pc, li, REDO);
            }
        }
    }

    /**
     * 撤销所有未完成（正在进行）的事务
     */
    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            long xid;
            if (isInertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                xid = li.xid;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                xid = li.xid;
            }
            if (tm.isActive(xid)) {
                if (!logCache.containsKey(xid))
                    logCache.put(xid, new ArrayList<>());
                logCache.get(xid).add(log);
            }
        }

        // 倒序undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInertLog(log))
                    doInsertLog(pc, parseInsertLog(log), UNDO);
                else
                    doUpdateLog(pc, parseUpdateLog(log), UNDO);
            }
            tm.abort(entry.getKey());  // 中止当前事务
        }
    }

    /**
     * 根据 flag 对 Insert 日志进行重做/撤销
     */
    private static void doInsertLog(PageCache pc, InsertLogInfo li, int flag) {
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (pg == null) Panic.panic(Error.PageIsNullException);
        try {
            if (flag == UNDO)  // 如果标志位为UNDO，将数据项设置为无效
                DataItem.setDataItemRawInvalid(li.raw);
                // todo BUG？
            else
                PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }

    private static void doUpdateLog(PageCache pc, UpdateLogInfo li, int flag) {
        byte[] raw = flag == REDO ? li.newRaw : li.oldRaw;
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (pg == null) Panic.panic(Error.PageIsNullException);
        try {
            PageX.recoverUpdate(pg, raw, li.offset);
        } finally {
            pg.release();
        }

    }

    /**
     * 根据日志恢复数据库
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering");

        lg.rewind();

        // 获取当前数据库的最大页数
        int maxPage = 0;
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            int pgno;
            if (isInertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if (pgno > maxPage)
                maxPage = pgno;
        }
        if (maxPage == 0)
            maxPage = 1;

        pc.truncateByBgno(maxPage);
        System.out.println("Truncate to " + maxPage + " pages.");

        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transactions Over");

        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transactions Over");

        System.out.println("Recovery Over");
    }

    /**
     * 创建一个更新日志：Update：[LogType](1) [XID](8) [UID](8) [OldRaw] [NewRaw]
     */
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        // 把原来 SubArray 中指定区间的一部分数据（start 到 end）拷贝出来，形成一个全新的 byte[] 数组，不再使用原来的引用，避免后续修改时可能影响全局！
        // 操作方式	                            是否拷贝数据？	        是否和原数组共享？	        是否安全独立？
        // SubArray.raw	                        ❌ 否	            ✅ 是	                ❌ 否（共享引用）
        // Arrays.copyOfRange(...start, end)	✅ 是	            ❌ 否	                ✅ 是（完全独立）
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 创建一个插入日志：Insert：[LogType](1) [XID](8) [Pgno](4) [Offset](2) [Raw]
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logType = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logType, xidRaw, pgnoRaw, offsetRaw, raw);
    }
}
