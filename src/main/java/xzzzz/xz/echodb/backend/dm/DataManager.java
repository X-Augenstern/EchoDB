package xzzzz.xz.echodb.backend.dm;

import xzzzz.xz.echodb.backend.dm.dataItem.DataItem;
import xzzzz.xz.echodb.backend.dm.logger.Logger;
import xzzzz.xz.echodb.backend.dm.page.PageOne;
import xzzzz.xz.echodb.backend.dm.pageCache.PageCache;
import xzzzz.xz.echodb.backend.tm.TransactionManager;

public interface DataManager {

    /**
     * 读
     */
    DataItem read(long uid) throws Exception;

    /**
     * 插入，并返回新插入的数据项的uid
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * 关闭：需要执行缓存和日志的关闭流程，还需要设置第一页的字节校验
     */
    void close();


    /**
     * 从空文件创建DM
     */
    static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);  // 创建一个PageCache实例
        Logger lg = Logger.create(path);  // 创建一个Logger实例
        DataManagerImpl dm = new DataManagerImpl(tm, pc, lg);  // 创建一个DataManagerImpl实例
        dm.initPageOne();
        return dm;
    }

    /**
     * 从已有文件创建DM
     */
    static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);  // 打开一个PageCache实例
        Logger lg = Logger.open(path);  // 打开一个Logger实例
        DataManagerImpl dm = new DataManagerImpl(tm, pc, lg);  // 创建一个DataManagerImpl实例
        if (!dm.loadCheckPageOne()) {  // 校验失败，说明上次非正常关闭数据库，进行恢复操作
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);  // 设置PageOne为打开状态
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}
