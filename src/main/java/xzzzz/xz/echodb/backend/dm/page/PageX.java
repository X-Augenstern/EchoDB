package xzzzz.xz.echodb.backend.dm.page;

import xzzzz.xz.echodb.backend.dm.pageCache.PageCache;
import xzzzz.xz.echodb.backend.utils.Parser;

import java.util.Arrays;

/**
 * 每个页面（page）在它的起始处（比如前两个字节）存了一个 2 字节的无符号整数，这个整数表示某种位置偏移，常见的是：空闲空间的起始位置 / 插入指针 / slot 表偏移等
 * <p>
 * 每个页面大小是 8192 字节（8KB），所以页内的所有内容、记录、指针、元数据都只能存在这 8192 字节以内
 * <p>
 * 2 字节（16 位）的无符号数最多能表示的范围：0 ～ 65535，比 8192（页大小）大很多，所以用 2 字节（无符号）来表示页内偏移是完全足够的
 * <p>
 * 数据页最多只有 8192 字节大，所以只需要用 2 字节就能表示页内任何一个位置的偏移（从 0 到 8191），这就是为什么数据页的开头会用 2 字节无符号整数来表示空闲空间位置或记录偏移的原因
 * <p>
 * 对于普通页，基本上都是围绕着 FSO（Free Space Offset） 进行管理的
 * <p>
 * +----------------------+ ← 偏移 0
 * | Free Space Offset (2字节) | ← 通常为 2字节，值比如：120
 * +----------------------+
 * | Number of Records (2字节)|
 * +----------------------+
 * | Slot Directory        |
 * | ...                   |
 * +----------------------+
 * | Record Data Area      |
 * | ...                   |
 * +----------------------+
 * | 空闲空间              |
 * +----------------------+ ← 偏移 8192（页末）
 * 最前面的 Free Space Offset 就是 2 字节
 * 它告诉我们：“记录区目前写到哪里了”
 * 也可能是记录插入起点、slot表位置、表尾指针等
 */
public class PageX {

    private static final short OF_FREE = 0;  // short：2 字节（16 位），有符号，范围是 -32768 ~ 32767

    private static final short OF_DATA = 2;  // 默认已经写到第二个字节处了

    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /**
     * 初始化第X页，并在头部设置2字节的FSO
     * @return 第X页的8K字节数组
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    /**
     * 设置空闲空间偏移量
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取pg的空闲空间偏移量（当前页开始记录数据的偏移位置）
     */
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    /**
     * 获取空闲空间偏移量
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, OF_FREE, OF_DATA));
    }

    /**
     * 获取页面的空闲空间大小
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    /**
     * 将raw插入pg中，返回插入位置
     * 其实就是把raw复制到页面当前的偏移位置后，并更新FSO
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(raw, (short) (offset + raw.length));
        return offset;
    }

    /**
     * 将raw插入pg中的指定offset位置，并将pg的offset设置为较大的offset
     * <p>
     * 用于在数据库崩溃后重新打开时，恢复例程插入数据使用
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        short rawOffset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        if (rawOffset < offset + raw.length)
            setFSO(pg.getData(), (short) (offset + raw.length));
    }

    /**
     * 将raw插入pg中的指定offset位置，不更新FSO
     * <p>
     * 用于在数据库崩溃后重新打开时，恢复例程修改数据使用
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
