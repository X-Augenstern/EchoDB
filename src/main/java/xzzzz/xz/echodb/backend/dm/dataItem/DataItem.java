package xzzzz.xz.echodb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import xzzzz.xz.echodb.backend.common.SubArray;
import xzzzz.xz.echodb.backend.dm.DataManagerImpl;
import xzzzz.xz.echodb.backend.dm.page.Page;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.backend.utils.UidUtil;

import java.util.Arrays;

public interface DataItem {

    /**
     * 返回数据项中的数据部分，返回的是原始数据的引用，而不是数据的拷贝
     * 获取 dataItem 的 [Data] 部分
     */
    SubArray data();

    /**
     * 在修改数据项之前调用，用于锁定数据项并保存原始数据
     * 写锁定，脏页面，把原始数据拷贝至旧的原始数据
     */
    void before();

    /**
     * 在需要撤销修改时调用，用于恢复原始数据并解锁数据项
     * 把旧的原始数据拷贝至原始数据，写锁定解除
     */
    void unBefore();

    /**
     * 在修改完成之后调用，用于记录日志并解锁数据项
     * 修改操作通过上层的tbm实现，先删除一条记录，再插入一条新记录，插入新记录调用 DM 插入
     * 所以在删除记录的时候调用after方法先写了日志，插入的时候才进行了数据的修改
     * <p>
     * dm记录日志，写锁定解除
     */
    void after(long xid);

    /**
     * 在使用完 DataItem 后，也应当及时调用 release() 方法，释放掉 DataItem 的缓存（由 DM 缓存 DataItem）
     */
    void release();

    /**
     * 写锁定
     */
    void lock();

    /**
     * 写锁定解除
     */
    void unlock();

    /**
     * 读锁定
     */
    void rLock();

    /**
     * 读锁定解除
     */
    void rUnLock();

    Page getPage();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    /**
     * 创建 dataItem 结构，如下：
     * [ValidFlag] [DataSize] [Data]
     * ValidFlag 1字节，0为合法，1为非法
     * DataSize  2字节，标识Data的长度
     */
    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];  // 创建一个长度为 1 的数组，里面的值默认为 0
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 从页面的 offset 位置解析 dataItem 数据项
     */
    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);  // 整个 dataItem 的长度
        long uid = UidUtil.parseToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], dm, uid, pg);
    }

    /**
     * 该条 dataItem 的有效位设置为无效，来进行逻辑删除
     */
    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
