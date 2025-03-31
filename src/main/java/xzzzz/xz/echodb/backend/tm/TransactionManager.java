package xzzzz.xz.echodb.backend.tm;


import xzzzz.xz.echodb.backend.utils.FileInfo;
import xzzzz.xz.echodb.backend.utils.FileUtil;
import xzzzz.xz.echodb.backend.utils.Panic;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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

    /**
     * 创建VM并写空XID文件头
     */
    static TransactionManagerImpl create(String path) {
        FileInfo fi = access(path, FileUtil.Mode.CREATE);
        FileChannel fc = fi.getFc();

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(fi.getRaf(), fc);
    }

    static TransactionManagerImpl open(String path) {
        FileInfo fi = access(path, FileUtil.Mode.OPEN);
        return new TransactionManagerImpl(fi.getRaf(), fi.getFc());
    }

    private static FileInfo access(String path, FileUtil.Mode mode) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        return FileUtil.checkFileAndBuildInfo(f, mode);
    }
}
