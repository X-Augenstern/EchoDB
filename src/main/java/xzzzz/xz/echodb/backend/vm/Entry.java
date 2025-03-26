package xzzzz.xz.echodb.backend.vm;

import com.google.common.primitives.Bytes;
import xzzzz.xz.echodb.backend.common.SubArray;
import xzzzz.xz.echodb.backend.dm.dataItem.DataItem;
import xzzzz.xz.echodb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出 Entry 结构
 * [XMIN] [XMAX] [DATA]
 * XMIN 是创建该条记录（版本）的事务编号：xid - 8字节
 * XMAX 是删除该条记录（版本）的事务编号：8字节
 * DATA 就是这条记录持有的数据
 * <p>
 * XMIN 应当在版本创建时填写，而 XMAX 则在版本被删除，或者有新版本出现时填写。
 * XMAX 这个变量，也就解释了为什么 DM 层不提供删除操作，当想删除一个版本时，只需要设置其 XMAX，这样，这个版本对每一个 XMAX 之后的事务都是不可见的，也就等价于删除了。
 * <p>
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 * 其中 [Data] 部分就是 Entry 结构：
 * [ ValidFlag | DataSize | Xmin | Xmax | Data... ]
 * 1B          2B       8B     8B     nB
 * <p>
 * 对于一条记录来说，EchoDB 使用 Entry 类维护了其结构。
 * 虽然理论上，MVCC 实现了多版本，但是在实现中，VM 并没有提供 Update 操作，对于字段的更新操作由后面的表和字段管理（TBM）实现。
 * 所以在 VM 的实现中，一条记录只有一个版本。
 * 由于一条记录存储在一条 Data Item 中，所以 Entry 中保存一个 DataItem 的引用即可
 */
public class Entry {

    private static final int OF_XMIN = 0;

    private static final int OF_XMAX = OF_XMIN + 8;

    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;

    /**
     * DataItem对象，用来存储数据
     */
    private DataItem dataItem;

    private VersionManager vm;

    public static Entry newEntry(long uid, DataItem dataItem, VersionManager vm) {
        if (dataItem == null) return null;
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    /**
     * 加载一个Entry。首先从DM中读取DataItem，然后创建一个新的Entry
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
        return newEntry(uid, di, vm);
    }

    /**
     * 生成记录格式数据
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];  // 创建一个空的8字节数组，等待版本修改或删除时才修改
        return Bytes.concat(xmin, xmax, data);
    }

    /**
     * 以拷贝（独立副本/非引用）的形式返回 entry 的 [Data] 部分
     */
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];  // Entry 部分去除 Xmin、Xmax 后真正的 Data
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置删除版本的事务编号
     */
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);  // 生成一个修改日志
        }
    }

    public long getUid() {
        return uid;
    }

    public void remove() {
        dataItem.release();
    }

    /**
     * 移除一个Entry
     */
    public void release() {
        ((VersionManagerImpl) vm).releaseEntry(this);
    }
}
