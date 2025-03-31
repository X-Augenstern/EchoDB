package xzzzz.xz.echodb.backend.tbm;

import com.google.common.primitives.Bytes;
import xzzzz.xz.echodb.backend.parser.statement.*;
import xzzzz.xz.echodb.backend.tm.TransactionManagerImpl;
import xzzzz.xz.echodb.backend.utils.Panic;
import xzzzz.xz.echodb.backend.utils.ParseStringRes;
import xzzzz.xz.echodb.backend.utils.Parser;
import xzzzz.xz.echodb.commen.Error;

import java.util.*;

/**
 * Table 维护了表结构
 * <p>
 * 二进制结构如下：
 * <p>
 * [TableName](4+n) [NextTableUid](8) [Field1Uid](8) [Field2Uid](8) ...[FieldNUid](8) （存储于 Entry 的 [Data] 部分）
 * <p>
 * - 为了明确字符串的存储边界，采用了一种规定的字符串存储方式，即在字符串数据之前存储了字符串的长度信息。
 * <p>
 * - [StringLength](4) [StringData](n)
 * <p>
 * <p>
 * Select/Update/Insert 使用的 Entry 的 [Data] 部分是按字段名顺序存储字段值
 */
public class Table {

    /**
     * 表管理器，用于管理数据库表
     */
    TableManager tbm;

    /**
     * 表的唯一标识符
     */
    long uid;

    /**
     * 表的名称
     */
    String name;

    /**
     * 表的状态
     */
    byte status;

    /**
     * 下一个表的唯一标识符
     */
    long nextUid;

    /**
     * 表的字段列表
     */
    List<Field> fields = new ArrayList<>();

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 解析原始字节数组并设置表名、下一个表的唯一标识符和表的字段列表
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;

        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    /**
     * 根据表uid从持久化存储中读取对应的 entry 的 [Data] 部分，并解析为Table对象
     */
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl) tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);  // 使用表管理器的版本管理器从数据库中读取指定uid的 entry 的 [Data] 部分
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 将当前Table对象持久化到存储中
     */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];  // 创建一个长度为 0 的空的字节数组
        for (Field f : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(f.uid));
        }
        uid = ((TableManagerImpl) tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * Create
     * <p>
     * 创建一个新的Table对象并持久化到存储中
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for (int j = 0; j < create.index.length; j++) {
                if (fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }
        return tb.persistSelf(xid);
    }

    class CalWhereRes {
        long l0, r0, l1, r1;

        /**
         * 是否是单一条件
         */
        boolean single;
    }

    /**
     * 计算 where 语句中的条件（logicOp），并将计算结果保存在 CalWhereRes 对象中
     */
    private CalWhereRes calWhere(Field f, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch (where.logicOp) {
            case "" -> {
                res.single = true;
                FieldCalRes r = f.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
            }
            case "or" -> {
                res.single = false;
                FieldCalRes r = f.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = f.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
            }
            case "and" -> {
                res.single = true;
                FieldCalRes r = f.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = f.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                if (res.l1 > res.l0) res.l0 = res.l1;  // str2Uid 是哈希，不能保证有序性
                if (res.r1 < res.r0) res.r0 = res.r1;
            }
            default -> throw Error.InvalidLogOpException;
        }
        return res;
    }

    /**
     * Where
     * <p>
     * 解析 where 子句并返回满足条件的 uid 列表
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0, r0, l1 = 0, r1 = 0;
        boolean single;
        Field f = null;

        // 如果 where 子句为空，则搜索所有记录
        if (where == null) {
            for (Field fd : fields) {  // 寻找第一个有索引的字段（只支持已索引字段作为 Where 的条件）
                if (fd.isIndexed()) {
                    f = fd;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field fd : fields) {
                if (fd.fieldName.equals(where.singleExp1.field)) {
                    if (!fd.isIndexed())  // 只支持已索引字段作为 Where 的条件
                        throw Error.FieldNotIndexedException;
                    f = fd;
                    break;
                }
            }
            if (f == null)
                throw Error.FieldNotFoundException;
            CalWhereRes res = calWhere(f, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }

        List<Long> uids = f.search(l0, r0);
        if (!single) {
            List<Long> tmp = f.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    /**
     * Delete
     * <p>
     * 删除
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if (((TableManagerImpl) tbm).vm.delete(xid, uid))
                count++;
        }
        return count;
    }

    /**
     * 将一段原始的字节数组 raw 按照字段定义 fields 的顺序逐个解析，并返回一个 Map<字段名, 字段值> 的键值对结构
     * <p>
     * raw里面按字段名顺序存储字段值
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field f : fields) {
            Field.ParseValueRes r = f.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(f.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    /**
     * 按字段名顺序依次把字段值转化为字节数组并拼接起来
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field f : fields) {
            raw = Bytes.concat(raw, f.value2Raw(entry.get(f.fieldName)));
        }
        return raw;
    }

    /**
     * Update
     * <p>
     * 更新
     * <p>
     * 此处 entry 的 [Data] 部分（raw）里面按字段名顺序存储字段值
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field f = null;
        for (Field fd : fields) {
            if (fd.fieldName.equals(update.fieldName)) {
                f = fd;
                break;
            }
        }
        if (f == null)
            throw Error.FieldNotFoundException;

        Object value = f.string2Value(update.value);  // 更新后的值
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);  // 读取 entry 的 [Data] 部分
            if (raw == null) continue;
            ((TableManagerImpl) tbm).vm.delete(xid, uid);  // 先删除记录
            Map<String, Object> entry = parseEntry(raw);  // Map<字段名, 字段值> 的键值对结构
            entry.put(f.fieldName, value);  // 更新值
            raw = entry2Raw(entry);  // 重新转为字节数组
            long uuid = ((TableManagerImpl) tbm).vm.insert(xid, raw);  // 插入完成更新
            count++;

            for (Field fd : fields) {
                if (fd.isIndexed())
                    fd.insert(entry.get(fd.fieldName), uuid);  // 将索引字段的值转化为key，与uid一起作为新节点往 B+ 树根节点递归插入
            }
        }
        return count;
    }

    /**
     * 将 Map<字段名, 字段值> 的键值对结构的字段值按照字段名的顺序以字符串形式输出
     * <p>
     * [val1, val2, ...]
     */
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            sb.append(f.printValue(entry.get(f.fieldName)));
            if (i == fields.size() - 1)
                sb.append("]");
            else
                sb.append(", ");
        }
        return sb.toString();
    }

    /**
     * Select
     * <p>
     * 将满足where条件的raw的字段值全部组合成字符串
     * <p>
     * 此处 entry 的 [Data] 部分（raw）里面按字段名顺序存储字段值
     */
    public String read(long xid, Select select) throws Exception {
        List<Long> uids = parseWhere(select.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 按字段名顺序依次将字符串数组的值转化为相应类型，并构建 Map<字段名, 字段值>
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size())
            throw Error.InvalidValuesException;
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            entry.put(f.fieldName, f.string2Value(values[i]));
        }
        return entry;
    }

    /**
     * Insert
     * <p>
     * 插入
     * <p>
     * 此处 entry 的 [Data] 部分（raw）里面按字段名顺序存储字段值
     */
    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl) tbm).vm.insert(xid, raw);
        for (Field f : fields) {
            if (f.isIndexed())
                f.insert(entry.get(f.fieldName), uid);  // 将索引字段的值转化为key，与uid一起作为新节点往 B+ 树根节点递归插入
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for (Field f : fields) {
            sb.append(f.toString());
            if (f == fields.get(fields.size() - 1))
                sb.append("}");
            else sb.append(", ");
        }
        return sb.toString();
    }
}
