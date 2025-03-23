package xzzzz.xz.echodb.backend.dm.page;

import xzzzz.xz.echodb.backend.dm.pageCache.PageCache;
import xzzzz.xz.echodb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 数据库文件的第一页，用于做一些特殊用途，比如存储一些元数据，用于启动检查等
 * 在 EchoDB 的第一页，只是用来做启动检查（ValidCheck）
 * <p>
 * 1. 每次数据库启动时，会生成一串随机字节，存储在 100~107 字节
 * 2. 在正常数据库关闭时，会将这串字节拷贝到第一页的 108~115 字节
 * 3. 数据库每次启动时，都会检查第一页两处的字节是否相同；用来判断上次是否正常关闭，是否需要进行数据的恢复流程
 */
public class PageOne {

    private static final int OF_VC = 100;

    private static final int LEN_VC = 8;

    /**
     * 初始化第一页，并随机生成8字节的校验码，拷贝到第一页的 100~107 字节
     * @return 第一页的8K字节数组
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 设置"ValidCheck"为打开状态
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 随机生成8字节的数据，并拷贝到第一页的 100~107 字节
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 设置"ValidCheck"为关闭状态
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 将raw数组中从OF_VC开始的LEN_VC个元素复制到raw数组中从OF_VC+LEN_VC开始的位置
     * 即 100~107 拷贝到 108~115
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    /**
     * 检查"ValidCheck"是否有效
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 比较 100~107 和 108~115 处字节是否相等
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
    }
}
