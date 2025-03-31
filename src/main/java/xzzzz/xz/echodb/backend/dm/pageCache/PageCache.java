package xzzzz.xz.echodb.backend.dm.pageCache;

import xzzzz.xz.echodb.backend.dm.page.Page;
import xzzzz.xz.echodb.backend.utils.FileInfo;
import xzzzz.xz.echodb.backend.utils.FileUtil;

import java.io.File;

/**
 * 定义了页面缓存的接口，包括新建页面、获取页面、释放页面缓存、关闭缓存、根据最大页号截断缓存、获取所有页面数量以及刷新页面等方法
 */
public interface PageCache {

    /**
     * 新建页面
     *
     * @return 当前页号
     */
    int newPage(byte[] initData);

    /**
     * 根据页码获取页面
     */
    Page getPage(int pgno) throws Exception;

    /**
     * 关闭缓存
     */
    void close();

    /**
     * 释放页面缓存
     */
    void release(Page page);

    /**
     * 根据最大页号截断缓存
     */
    void truncateByBgno(int maxPgno);

    /**
     * 获取数据库文件中的所有页面数量
     */
    int getTotalPageNumber();

    /**
     * 将某个 Page 的数据从内存写回文件的操作，确保数据被同步保存到磁盘中，保证持久化
     */
    void flushPage(Page pg);

    /**
     * 参考大部分数据库的设计，将默认数据页大小定为 8K。如果想要提升向数据库写入大量数据情况下的性能的话，也可以适当增大这个值
     */
    int PAGE_SIZE = 1 << 13;

    /*
     接口中的 static 方法属于接口本身，不会被实现类继承或自动“默认使用”

     情况	                            能否继承/使用？
     接口中的 static 方法	                ❌ 不能被实现类继承（必须用接口名调用）
     接口中的 default 方法	            ✅ 可以被实现类继承，并可选择重写
     接口中的 public abstract 方法（常规）	✅ 必须实现

     这种模式是一种“静态工厂方法模式”，在接口中定义静态方法创建实现类对象
     好处：调用时不暴露实现类，解耦了接口与实现。
     */

    /**
     * 第一次初始化数据库文件时调用得到 PageCacheImpl 实例
     * <p>
     * 静态工厂方法模式返回实现类
     */
    static PageCacheImpl create(String path, long memory) {
        return access(path, memory, FileUtil.Mode.CREATE);
    }

    /**
     * 用于后续启动时打开已存在的数据库文件
     */
    static PageCacheImpl open(String path, long memory) {
        return access(path, memory, FileUtil.Mode.OPEN);
    }

    private static PageCacheImpl access(String path, long memory, FileUtil.Mode mode) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        FileInfo fi = FileUtil.checkFileAndBuildInfo(f, mode);
        return new PageCacheImpl((int) memory / PAGE_SIZE, fi.getRaf(), fi.getFc());
    }
}
