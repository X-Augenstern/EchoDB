package xzzzz.xz.echodb.backend.utils;

import java.nio.ByteBuffer;

public class Parser {

    /**
     * 将一个字节数组（byte[] buf）中的前8个字节解析为一个 long 类型的整数值
     * <p>
     * long 在 Java 中始终是 8 字节（64 位），每两个十六进制位是一个字节（8位）
     * 十六进制的一位 ≈ 4 位二进制 -> 1个十六进制位 = 4个二进制位（4 bit） | 2个十六进制位 = 8个二进制位（8 bit）= 1 byte
     * <p>
     * 有一个 int（4字节）或 long（8字节）值：long value = 0x1122334455667788L;
     * 这个 long 类型的值在内存中的十六进制表示是：11 22 33 44 55 66 77 88
     * 在大端序（Big Endian）中，内存布局是这样的（从低地址到高地址），即：高位在前，低位在后
     * 地址:   0   1   2   3   4   5   6   7
     * 内容:  11  22  33  44  55  66  77  88
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);  // 将 buf 数组的前8个字节（从索引0开始）包装成一个 ByteBuffer 对象
        return buffer.getLong();  // 从 ByteBuffer 中读取8个字节，并按照默认的大端序（big-endian）将它们组合成一个 long 类型的数值
    }

    /**
     * long val = 0x1122334455667788L;
     * byte[] bytes = long2Byte(val);
     * -> [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]
     */
    public static byte[] long2Byte(long val) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(val).array();  // 把val写入这个ByteBuffer中（按照大端序，高位在前），返回内部的byte[]数组
    }
}
