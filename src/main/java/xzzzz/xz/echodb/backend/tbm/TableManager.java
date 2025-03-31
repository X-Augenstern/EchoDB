package xzzzz.xz.echodb.backend.tbm;

import xzzzz.xz.echodb.backend.dm.DataManager;
import xzzzz.xz.echodb.backend.parser.statement.*;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.backend.vm.VersionManager;

public interface TableManager {

    /**
     * Begin
     */
    BeginRes begin(Begin begin);

    /**
     * Commit
     */
    byte[] commit(long xid) throws Exception;

    /**
     * Abort
     */
    byte[] abort(long xid);

    /**
     * Show
     * <p>
     * 把当前所有缓存的表（table）信息，以及事务 xid 关联的表信息，转换为字符串，然后返回其字节数组表示（byte[]）
     * <p>
     * 如果没有事务相关的表，直接返回换行
     */
    byte[] show(long xid);

    /**
     * Create
     * <p>
     * 创建新表并将其作为第一张表，将其uid写入 Booter 文件，添加进表缓存、事务表缓存
     */
    byte[] create(long xid, Create create) throws Exception;

    /**
     * Insert
     */
    byte[] insert(long xid, Insert insert) throws Exception;

    /**
     * Select
     */
    byte[] read(long xid, Select select) throws Exception;

    /**
     * Update
     */
    byte[] update(long xid, Update update) throws Exception;

    /**
     * Delete
     */
    byte[] delete(long xid, Delete delete) throws Exception;

    static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
