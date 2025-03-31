package xzzzz.xz.echodb.backend.tbm;

import com.google.common.primitives.Bytes;
import xzzzz.xz.echodb.backend.im.BPlusTree;
import xzzzz.xz.echodb.backend.parser.statement.SingleExpression;
import xzzzz.xz.echodb.backend.tm.TransactionManagerImpl;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.utils.ParseStringRes;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.commen.Error;

import java.util.Arrays;
import java.util.List;

/**
 * field 表示字段信息
 * <p>
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid/bootUid] （存储于 Entry 的 [Data] 部分）
 * <p>
 * 如果field无索引，IndexUid为0
 * <p>
 * 1. 数据存储结构：表和字段的信息以二进制形式存储在数据库的 Entry 中。
 * <p>
 * 2. 字段信息表示：字段的二进制表示包含字段名（FieldName）、字段类型（TypeName）和索引UID（IndexUid）。
 * <p>
 * - 字段名和字段类型以及其他信息都以字节形式的字符串存储。
 * <p>
 * - [FieldName](4+n) [TypeName](4+n) [IndexUid/bootUid](8)
 * <p>
 * - 为了明确字符串的存储边界，采用了一种规定的字符串存储方式，即在字符串数据之前存储了字符串的长度信息。
 * <p>
 * - [StringLength](4) [StringData](n)
 * <p>
 * 3. 字段类型限定：字段的类型被限定为 int32、int64 和 string 类型。
 * <p>
 * 4. 索引表示：如果字段被索引，则IndexUid指向了索引二叉树的根节点；否则该字段的IndexUid为0。
 * <p>
 * 5. 读取和解析：通过唯一标识符（UID）从虚拟内存（VM）中读取字段信息，并根据上述结构解析该信息。
 */
public class Field {

    /**
     * 唯一标识符，用于标识每个Field对象
     */
    long uid;

    /**
     * Field对象所属的表
     */
    private Table tb;

    /**
     * 字段名，用于标识表中的每个字段
     */
    String fieldName;

    /**
     * 字段类型，用于标识字段的数据类型
     */
    String fieldType;

    /**
     * 索引，用于标识字段是否有索引，如果索引为0，表示没有索引（B+树的bootUid）
     */
    private long index;

    /**
     * B+树，用于存储索引，如果字段有索引，这个B+树会被加载
     */
    private BPlusTree bt;

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /**
     * 解析原始字节数组并设置字段名、字段类型和索引
     */
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;

        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;

        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        if (index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * 根据字段uid从持久化存储中读取对应的 entry 的 [Data] 部分，并解析为Field对象
     */
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl) tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;  // 断言原始字节数据不为null，如果为null，那么会抛出AssertionError
        return new Field(uid, tb).parseSelf(raw);  // 创建一个新的Field对象，并调用parseSelf方法解析原始字节数据
    }

    /**
     * int32 | int64 | string
     */
    private static void typeCheck(String fieldType) throws Exception {
        if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType))
            throw Error.InvalidFieldException;
    }

    /**
     * 将当前Field对象持久化到存储中
     */
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        // 插入成功后，会返回一个唯一的uid，将这个uid设置为当前Field对象的uid
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    /**
     * 创建一个新的Field对象并持久化到存储中
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        if (indexed) {
            long index = BPlusTree.create(((TableManagerImpl) tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    /**
     * 当前Field对象是否有索引
     */
    public boolean isIndexed() {
        return index != 0;
    }

    /**
     * 根据字段类型把val转化为long（B+树中的key）
     */
    public long value2Key(Object val) {
        return switch (fieldType) {
            case "string" -> Parser.str2Uid((String) val);
            case "int32" -> (long) (int) val;
            case "int64" -> (long) val;
            default -> 0;
        };
    }

    /**
     * 往 B+ 树根节点递归插入新节点
     */
    public void insert(Object val, long uid) throws Exception {
        long uKey = value2Key(val);
        bt.insert(uKey, uid);
    }

    /**
     * 在 B+树中搜索 leftKey - rightKey 范围内的所有子节点uid
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    /**
     * 根据字段类型把字符串转化为相应类型
     */
    public Object string2Value(String str) {
        return switch (fieldType) {
            case "int32" -> Integer.parseInt(str);
            case "int64" -> Long.parseLong(str);
            case "string" -> str;
            default -> null;
        };
    }

    /**
     * 根据字段类型把val转化为相应类型的字节数组
     */
    public byte[] value2Raw(Object val) {
        return switch (fieldType) {
            case "int32" -> Parser.int2Byte((int) val);
            case "int64" -> Parser.long2Byte((long) val);
            case "string" -> Parser.string2Byte((String) val);
            default -> null;
        };
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    /**
     * 根据字段类型把字节数组转化为相应类型的值、偏移量
     */
    public ParseValueRes parseValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32" -> {
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
            }
            case "int64" -> {
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
            }
            case "string" -> {
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
            }
        }
        return res;
    }

    /**
     * 根据字段类型把val转化为字符串
     */
    public String printValue(Object val) {
        String str = null;
        switch (fieldType) {
            case "int32" -> str = String.valueOf((int) val);
            case "int64" -> str = String.valueOf((long) val);
            case "string" -> str = (String) val;
        }
        return str;
    }

    @Override
    public String toString() {
        return "(" +
                fieldName +
                ", " +
                fieldType +
                (index != 0 ? ", Index" : ", NoIndex") +
                ")";
    }

    /**
     * SingleExpression
     * <p>
     * 根据比较操作符（<, =, >）和字段类型，计算出一个区间，并返回一个 FieldCalRes 对象，包含 left 和 right 值。
     * <p>
     * <: left 为 0，right 为转换后的值减去 1。
     * <p>
     * =: left 和 right 相等，均为转换后的值。
     * <p>
     * >: right 为最大值，left 为转换后的值加 1。
     */
    public FieldCalRes calExp(SingleExpression exp) {
        Object v;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp) {
            case "<" -> {
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Key(v);
                if (res.right > 0)
                    res.right--;
            }
            case "=" -> {
                v = string2Value(exp.value);
                res.left = value2Key(v);
                res.right = res.left;
            }
            case ">" -> {
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Key(v) + 1;
            }
        }
        return res;
    }
}
