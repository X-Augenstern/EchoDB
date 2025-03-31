package xzzzz.xz.echodb.backend.parser;

import xzzzz.xz.echodb.commen.Error;

/**
 * Tokenizer类用于对语句进行逐字节解析，根据空白符或者特定的词法规则，将语句切割成多个token。
 * <p>
 * 提供了peek()和pop()方法，方便取出Token进行解析。
 */
public class Tokenizer {

    /**
     * SQL 语句
     */
    private byte[] stat;

    /**
     * 当前指针位置
     */
    private int pos;

    /**
     * 当前token
     */
    private String currentToken;

    /**
     * 当前token是否需要刷新
     */
    private boolean flushToken;

    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 换行/缩进/空格
     */
    public static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }

    /**
     * > < = * , ( )
     */
    public static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' ||
                b == '*' || b == ',' ||
                b == '(' || b == ')');
    }

    /**
     * 0-9
     * <p>
     * 比较过程：
     * 字符常量（char）会被转换为 int：
     * 字符在 Java 中本质上是一个 16 位的无符号整数，范围是 0 到 65535，表示字符的 Unicode 值。
     * Java 会将字符常量（如 '0' 和 '9'）转换为其对应的 Unicode 值，这些值是整数。例如：
     * '0' 转换为 48（int 类型）。
     * '9' 转换为 57（int 类型）。
     * <p>
     * 字节（byte）会被提升为 int：
     * 在比较时，byte 类型会被自动转换为 int 类型，这是 Java 的自动类型转换规则之一。byte 是 8 位有符号整数，范围是 -128 到 127，int 是 32 位，因此 byte 会被扩展到 32 位以进行比较。
     * 例如，如果 b = 50（byte），它会被自动转换为 int 类型 50。
     * <p>
     * 最终比较：
     * 比较时，Java 会将 byte（转换为 int）与字符常量（已转换为 int）进行比较。
     * 例如，比较 (b >= '0' && b <= '9')，实际上等同于 (b >= 48 && b <= 57)，其中 b 会被转换为 int 类型。
     */
    public static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    /**
     * a-z A-Z
     */
    public static boolean isAlphaBeta(byte b) {
        return (b >= 'a' && b <= 'z' ||
                b >= 'A' && b <= 'Z');
    }

    /**
     * 获取当前指针指向的字节
     */
    private Byte peekByte() {
        if (pos == stat.length)
            return null;
        return stat[pos];
    }

    /**
     * 指针往后移一位
     */
    private void popByte() {
        pos++;
        if (pos > stat.length)
            pos = stat.length;
    }

    /**
     * 处理被引号包围的字符串
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();  // 获取当前字节，这应该是一个引号
        popByte();
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            if (b == null) {  // 如果当前字节为空，设置错误状态为无效的命令异常
                err = Error.InvalidCommandException;
                throw err;
            }
            if (b == quote) {  // 如果当前字节是引号
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));  // 如果当前字节不是引号，将这个字节添加到StringBuilder中
            popByte();
        }
        return sb.toString();
    }

    /**
     * 获取下一个标记。标记是由字母、数字或下划线组成的字符串
     */
    private String nextTokenState() {
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            // 如果当前字节为空，或者不是字母、数字或下划线，那么结束循环
            if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if (b != null && isBlank(b))  // 如果当前字节是空白字符
                    popByte();
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    /**
     * 获取下一个元状态。元状态可以是一个符号、引号包围的字符串或者一个由字母、数字或下划线组成的标记
     */
    private String nextMetaState() throws Exception {
        while (true) {
            Byte b = peekByte();
            if (b == null)
                return "";
            if (!isBlank(b))
                break;  // 如果当前字节不是空白字符，跳出循环
            popByte();
        }
        byte b = peekByte();
        if (isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});  // 如果这个字节是一个符号，返回这个符号
        } else if (b == '"' || b == '\'') {
            return nextQuoteState();  // 如果这个字节是一个引号，返回引号内的字符串
        } else if (isAlphaBeta(b) || isDigit(b))
            return nextTokenState();  // 如果这个字节是一个字母或数字，返回由字母、数字或下划线组成的字符串
        else {
            err = Error.InvalidCommandException;
            throw err;
        }

    }

    /**
     * 获取下一个元状态。元状态可以是一个符号、引号包围的字符串或者一个由字母、数字或下划线组成的标记。如果存在错误，将抛出异常。
     */
    private String next() throws Exception {
        if (err != null)
            throw err;
        return nextMetaState();
    }

    /**
     * 如果当前token需要刷新，获取下一个token（一个符号、引号包围的字符串、一个由字母、数字或下划线组成的字符串）
     */
    public String peek() throws Exception {
        if (err != null)
            throw err;

        if (flushToken) {
            String token;
            try {
                token = next();
            } catch (Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    /**
     * 当前token需要刷新，这样下一次调用peek()时会生成新的token
     */
    public void pop() {
        flushToken = true;
    }

    /**
     * 返回新字节数组 [0-pos-1] [<< ] [pos-末尾]
     */
    public byte[] errStat() {
        byte[] res = new byte[stat.length + 3];
        // 把stat [0,pos-1] 的字节拷贝到res的 [0,pos-1] 去
        System.arraycopy(stat, 0, res, 0, pos);
        // getBytes() 会返回字符串中每个字符的字节表示：
        // 在这里，字符 "<" 的字节值是 60，字符 " " 的字节值是 32。所以 "<< " 会转换为一个字节数组：[60, 60, 32]
        // 把 [60, 60, 32] 拷贝到res的 [pos,pos+2] 去
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        // 把stat [pos,末尾] 拷贝到res的 [pos+3,末尾] 去
        System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
        return res;
    }
}
