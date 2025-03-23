package xzzzz.xz.echodb.backend.dm;

import xzzzz.xz.echodb.backend.common.AbstractCache;
import xzzzz.xz.echodb.backend.dm.dataItem.DataItem;
import xzzzz.xz.echodb.backend.dm.dataItem.DataItemImpl;
import xzzzz.xz.echodb.backend.dm.logger.Logger;
import xzzzz.xz.echodb.backend.dm.page.Page;
import xzzzz.xz.echodb.backend.dm.page.PageOne;
import xzzzz.xz.echodb.backend.dm.page.PageX;
import xzzzz.xz.echodb.backend.dm.pageCache.PageCache;
import xzzzz.xz.echodb.backend.dm.pageIndex.PageIndex;
import xzzzz.xz.echodb.backend.dm.pageIndex.PageInfo;
import xzzzz.xz.echodb.backend.tm.TransactionManager;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.utils.UidUtil;
import xzzzz.xz.echodb.commen.Error;

/**
 * DataManager（DM）是数据库管理系统中的一层，主要负责底层数据的管理和操作。其功能和作用包括：
 * <p>
 * 1. 数据缓存和管理：实现了对 DataItem 对象的缓存管理，通过缓存管理，可以提高数据的访问效率，并减少对底层存储的频繁访问，从而提高系统的性能。
 * 2. 数据访问和操作：提供了读取、插入和修改等数据操作方法，上层模块可以通过这些方法对数据库中的数据进行操作和管理。
 * 3. 事务管理：支持事务的管理，通过事务管理，可以保证对数据的修改是原子性的，并且在事务提交或回滚时能够保持数据的一致性和完整性。
 * 4. 日志记录和恢复：在数据修改操作前后会执行一系列的流程，包括日志记录和数据恢复等操作，以确保数据的安全性和可靠性，即使在系统崩溃或异常情况下也能够保证数据的完整性。
 * 5. 页面索引管理：实现了页面索引管理功能，通过页面索引可以快速定位到合适的空闲空间，从而提高数据插入的效率和性能。
 * 6. 文件初始化和校验：在创建和打开数据库文件时，会进行文件的初始化和校验操作，以确保文件的正确性和完整性，同时在文件关闭时会执行相应的清理操作。
 * 7. 资源管理和释放：在关闭时会执行资源的释放和清理操作，包括缓存和日志的关闭，以及页面的释放和页面索引的清理等。
 * <p>
 * DataManager 在数据库管理系统中扮演着重要的角色，负责底层数据的管理和操作，为上层模块提供了方便的数据访问和操作接口，同时通过事务管理和日志记录等功能保证了数据的安全性和可靠性。
 * <p>
 * DM 直接管理数据库 DB 文件和日志文件。
 * DM 的主要职责有：
 * 1) 分页管理 DB 文件，并进行缓存；
 * 2) 管理日志文件，保证在发生错误时可以根据日志进行恢复；
 * 3) 抽象 DB 文件为 DataItem 供上层模块使用，并提供缓存。
 * DM 的功能其实可以归纳为两点：上层模块和文件系统之间的一个抽象层，向下直接读写文件，向上提供数据的包装；另外就是日志功能。
 * 可以注意到，无论是向上还是向下，DM 都提供了一个缓存的功能，用内存操作来保证效率。
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    private TransactionManager tm;

    PageCache pc;

    private Logger lg;

    private PageIndex pIndex;

    /**
     * 第一页
     */
    public Page pageOne;

    public DataManagerImpl(TransactionManager tm, PageCache pc, Logger lg) {
        super(0);
        this.tm = tm;
        this.pc = pc;
        this.lg = lg;
        this.pIndex = new PageIndex();
    }

    /**
     * DataManager 被创建时，需要获取所有页面并填充 PageIndex
     */
    public void fillPageIndex() {
        int pageNumber = pc.getTotalPageNumber();
        for (int i = 2; i <= pageNumber; i++) {  // 在 EchoDB 的第一页，只是用来做启动检查（ValidCheck）
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            if (pg == null) Panic.panic(Error.PageIsNullException);
            pIndex.add(i, PageX.getFreeSpace(pg));
            pg.release();
        }
    }

    /**
     * 在创建DM时初始化PageOne并写回磁盘
     */
    public void initPageOne() {
        int pgno = pc.newPage(PageOne.initRaw());
        assert pgno == 1;
        try {
            this.pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    /**
     * 在打开已有的DM文件时读取PageOne并校验
     */
    public boolean loadCheckPageOne() {
        try {
            this.pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 根据 uid 从缓存中读取 DataItem，并校验有效位
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    /**
     * 首先需要写入插入日志，接着才可以通过 PageX 插入数据，并返回插入位置的偏移。最后需要将页面信息重新插入 pageIndex
     *
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);  // 将输入的数据包装成DataItem的原始格式
        if (raw.length > PageX.MAX_FREE_SPACE)
            throw Error.DataTooLargeException;

        PageInfo pi = null;
        for (int i = 0; i < 5; i++) {  // 尝试5次找到一个可以容纳新数据项的页面
            pi = pIndex.select(raw.length);
            if (pi != null) break;  // 如果找到了合适的页面，跳出循环
            else {
                int newPgno = pc.newPage(PageX.initRaw());  // 如果没有找到合适的页面，创建一个新的页面，并将其添加到页面索引中
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi == null)
            throw Error.DatabaseBusyException;

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);  // 生成插入日志
            lg.log(log);  // 将日志写入日志文件

            short offset = PageX.insert(pg, raw);  // 在页面中插入新的数据项，并返回插入位置

            pg.release();  // 释放页面
            return UidUtil.parseToUid(pi.pgno, offset);  // 返回新插入的数据项的唯一标识符
        } finally {
            // 将页面重新添加到页面索引中
            if (pg != null)
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            else
                pIndex.add(pi.pgno, freeSpace);  // 这一页的空闲空间设置为0，这样后续的insert的时候就不会找到此页
        }
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        UidUtil.UidInfo uidInfo = UidUtil.parseUid(uid);
        short offset = uidInfo.getOffset();
        int pgno = uidInfo.getPgno();
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.release();
    }

    /**
     * 释放对 dataItem 资源的引用
     */
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    /**
     * 为 xid 生成 update 日志
     */
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        lg.log(log);
    }

    @Override
    public void close() {
        super.close();
        lg.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }
}
