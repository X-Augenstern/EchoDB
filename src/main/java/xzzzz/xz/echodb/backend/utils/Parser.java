package xzzzz.xz.echodb.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Parser {
    /*
      字节数组 -> ByteBuffer -> 数据类型：根据数据类型对应的字节数从字节数组中读取并创建字节缓冲区对象 ByteBuffer，从对象中获取对应的数据类型
      数据类型 -> ByteBuffer -> 字节数组：根据数据类型对应的字节数为字节缓冲区对象 ByteBuffer 分配相应空间，放入数据，.array()得到相应的字节数组
     */

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

    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    /**
     * 解析字节数组的前4个字节，前4个字节表示后面字符串的长度n，返回解码成的字符串以及4+n位置/偏移量
     */
    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));  // 将提取出来的字节数组转换为 String，得到原始的字符串
        return new ParseStringRes(str, length + 4);
    }

    /**
     * long val = 0x1122334455667788L;
     * byte[] bytes = long2Byte(val);
     * -> [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]
     */
    public static byte[] long2Byte(long val) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(val).array();  // 把val写入这个ByteBuffer中（按照大端序，高位在前），返回内部的byte[]数组
    }

    public static byte[] short2Byte(short val) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(val).array();
    }

    public static byte[] int2Byte(int val) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(val).array();
    }

    /**
     * 把字符串长度n转化为4个字节，再拼上由字符串编码成的字节数组本身
     */
    public static byte[] string2Byte(String val) {
        byte[] l = int2Byte(val.length());
        return Bytes.concat(l, val.getBytes());
    }

    /**
     * str -> byte -> uid
     */
    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for (byte b : key.getBytes()) {
            res = res * seed + b;
        }
        return res;
    }
}
