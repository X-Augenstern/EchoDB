package xzzzz.xz.echodb.backend.utils;

public class UidUtil {

    /**
     * 将 pgno、offset 转化为 uid
     * |------ 高 32 位 pgno ------|-- 中间 16 位（保留） --|-- 低 16 位 offset --|
     * |      页号（Page Number）  |        保留字段        |     页内偏移量       |
     */
    public static long parseToUid(int pgno, short offset) {
        long u0 = pgno;  // 左移 32 位 → 放到高位
        long u1 = offset;
        return u0 << 32 | u1;
    }

    /**
     * 封装 pgno + offset 的结构体
     */
    public static class UidInfo {
        private final int pgno;
        private final short offset;

        public UidInfo(int pgno, short offset) {
            this.pgno = pgno;
            this.offset = offset;
        }

        public int getPgno() {
            return this.pgno;
        }

        public short getOffset() {
            return this.offset;
        }
    }

    /**
     * 解析 uid 为 UidInfo（pgno + offset）
     */
    public static UidInfo parseUid(long uid) {
        // 1L << 16 = 0x0001_0000 = 65536
        // 65536 - 1 = 65535 = 0x0000_FFFF
        // (uid & 0xFFFF)：只保留低 16 位，其他全部置 0
        short offset = (short) (uid & ((1L << 16) - 1));
        // 运算符	名称	        含义                              符号位是否保留	左边补什么？	结果可能是负数吗？
        // <<	    左移	        所有位向左移动，右边补 0              不管符号	    右移补 0	    ✅ 取决于原数
        // >>>	    无符号右移	所有位向右移动，左边补 0              ❌ 不管符号	强制补 0	    ❌（一定变成正数）
        // >>	    有符号右移	所有位向右移动，左边补符号位（0 或 1）  ✅ 保留符号位	补原来的符号	✅（常用于有符号运算）
        // 🔹 << 是往左挪，值变大（乘法）
        // 🔹 >> 是带符号往右挪（除法，保符号）
        // 🔹 >>> 是不管三七二十一，右边挪完左边补 0（只能正数）
        // 🔹 <<< 是非法运算符，不存在
        /*
            1. << 左移（左边丢掉，右边补0）
            int x = 3;           // 二进制 00000000_00000000_00000000_00000011
            int y = x << 2;      //          => 00000000_00000000_00000000_00001100 = 12
            相当于 x * 2^2 = 3 * 4 = 12

            2. >> 有符号右移（保留符号位）
            int x = -8;          // 二进制：11111111_11111111_11111111_11111000
            int y = x >> 2;      //          => 11111111_11111111_11111111_11111110 = -2
            左边补的是 符号位（1），因为是负数，保持负数特性。

            3. >>> 无符号右移（始终补0）
            int x = -8;          // 二进制：11111111_11111111_11111111_11111000
            int y = x >>> 2;     //          => 00111111_11111111_11111111_11111110（正数！）
            左边强制补 0，结果会变成一个非常大的正整数。
         */
        uid >>>= 32;  // 丢掉低 32 位，只留下高 32 位
        int pgno = (int) (uid & ((1L << 32) - 1));  // 虽然右移完已经是 32 位了，但为了安全/语义清晰，它再用 & 0xFFFFFFFF 只保留低 32 位
        return new UidInfo(pgno, offset);
    }
}
