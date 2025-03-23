package xzzzz.xz.echodb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;


public class RandomUtil {

    /**
     * 生成指定长度的随机字节数组
     */
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        /*
        Java 的 byte（有符号）	    -128 到 127（共 256 个值）
        无符号字节（比如 C/网络协议）	0 到 255（共 256 个值）
         */
        r.nextBytes(buf);  // 将每个字节填充为一个随机值（-128 ~ 127 之间，因为 Java 的 byte 类型是带符号的 8 位整数）
        return buf;
    }
}
