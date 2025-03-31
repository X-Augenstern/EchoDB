package xzzzz.xz.echodb.backend.parser;

import xzzzz.xz.echodb.backend.parser.statement.*;
import xzzzz.xz.echodb.commen.Error;

import java.util.ArrayList;
import java.util.List;

/**
 * 实现了对类 SQL 语句的结构化解析，将语句中包含的信息封装为对应语句的类
 * <p>
 * 对外提供了Parse(byte[] statement)方法，用于解析语句。
 * <p>
 * 解析过程核心是调用Tokenizer类来分割Token，并根据词法规则将Token包装成具体的Statement类，并返回。
 * <p>
 * 解析过程相对简单，仅根据第一个Token来区分语句类型，并分别处理。
 */
public class Parser {

    /**
     * 解析输入的字节流，根据不同的标记（token）调用不同的解析方法，生成对应的语句对象。
     * <p>
     * throw 抛出异常 ➜ 中断当前函数执行。
     * 是否终止整个程序，要看有没有人去处理（try-catch）。
     * 如果没有处理，异常就会一直向上冒泡（调用栈逐层返回），直到崩溃。
     */
    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            switch (token) {
                case "begin" -> stat = parseBegin(tokenizer);
                case "commit" -> stat = parseCommit(tokenizer);
                case "abort" -> stat = parseAbort(tokenizer);
                case "create" -> stat = parseCreate(tokenizer);
                case "drop" -> stat = parseDrop(tokenizer);
                case "select" -> stat = parseSelect(tokenizer);
                case "insert" -> stat = parseInsert(tokenizer);
                case "delete" -> stat = parseDelete(tokenizer);
                case "update" -> stat = parseUpdate(tokenizer);
                case "show" -> stat = parseShow(tokenizer);
                default -> throw Error.InvalidCommandException;
            }
        } catch (Exception e) {
            statErr = e;
        }

        try {
            String next = tokenizer.peek();  // 获取下一个token
            if (!"".equals(next)) {  // SQL 语句会在每一种情况中都解析到底，如果到这里都还没有解析到底，那就说明语句里有错误（一种健壮性检测）
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch (Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }

        if (statErr != null)
            throw statErr;

        return stat;
    }

    /**
     * Begin SQL:
     * <p>
     * begin
     * <p>
     * begin isolation level (read committed / repeatable read)
     */
    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();

        if ("".equals(isolation))  // 仅 begin
            return begin;

        if (!"isolation".equals(isolation))  // + isolation
            throw Error.InvalidCommandException;
        tokenizer.pop();

        String level = tokenizer.peek();
        if (!"level".equals(level))  // + level
            throw Error.InvalidCommandException;
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if ("read".equals(tmp1)) {  // + read
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("committed".equals(tmp2)) {  // + committed
                tokenizer.pop();
                if (!"".equals(tokenizer.peek()))  // SQL 语句里有错误的尾部
                    throw Error.InvalidCommandException;
                return begin;
            } else
                throw Error.InvalidCommandException;
        } else if ("repeatable".equals(tmp1)) {  // + repeatable
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("read".equals(tmp2)) {  // + read
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if (!"".equals(tokenizer.peek()))  // SQL 语句里有错误的尾部
                    throw Error.InvalidCommandException;
                return begin;
            } else
                throw Error.InvalidCommandException;
        } else
            throw Error.InvalidCommandException;
    }

    /**
     * Commit SQL:
     * <p>
     * commit
     */
    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        return new Commit();
    }

    /**
     * Abort SQL:
     * <p>
     * abort
     */
    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        return new Abort();
    }

    /**
     * 首字符是字母或下划线，其他字符是字母、数字或下划线
     */
    private static boolean isName(String name) {
        if (name.isEmpty() || (!Tokenizer.isAlphaBeta(name.getBytes()[0]) && name.charAt(0) != '_'))
            return false;

        for (char c : name.toCharArray()) {
            byte tmp = String.valueOf(c).getBytes()[0];
            if (!(Tokenizer.isDigit(tmp) || Tokenizer.isAlphaBeta(tmp) || c == '_'))
                return false;
        }
        return true;
    }

    /**
     * int32 | int64 | string
     */
    private static boolean isType(String tp) {
        return ("int32".equals(tp) ||
                "int64".equals(tp) ||
                "string".equals(tp));
    }

    /**
     * Create SQL:
     * <p>
     * create table <table name>
     * <field name> <field type>
     * <field name> <field type>
     * ...
     * <field name> <field type>
     * (index <field name list>)
     * <p>
     * eg:
     * create table students
     * id int32,
     * name string,
     * age int32
     * (index id name)
     */
    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek()))  // + table
            throw Error.InvalidCommandException;
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();
        if (!isName(name))  // + 表名
            throw Error.InvalidCommandException;
        create.tableName = name;

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if ("(".equals(field))  // + (：break进入index部分
                break;
            if (!isName(field))
                throw Error.InvalidCommandException;

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if (!isType(fieldType))
                throw Error.InvalidCommandException;
            fNames.add(field);  // + 字段名
            fTypes.add(fieldType);  // + 字段类型

            tokenizer.pop();
            String next = tokenizer.peek();
            if (",".equals(next))
                continue;
            else if ("".equals(next))  // 没有index部分
                throw Error.TableNoIndexException;
            else if ("(".equals(next))
                break;
            else
                throw Error.InvalidCommandException;
        }
        create.fieldName = fNames.toArray(new String[0]);
        create.fieldType = fTypes.toArray(new String[0]);

        tokenizer.pop();
        if (!"index".equals(tokenizer.peek()))  // + index
            throw Error.InvalidCommandException;

        List<String> indexes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if (")".equals(field))  // + )
                break;
            if (!isName(field))
                throw Error.InvalidCommandException;
            else
                indexes.add(field);  // + 字段名
        }
        // List 类提供了一个 toArray() 方法，用于将 List 转换成数组
        // 调用 toArray() 时，需要提供一个包含目标类型的数组，以便 List 可以根据该数组的大小创建一个合适的数组
        // new String[indexes.size()] 创建了一个 String 类型的数组，并且数组的大小等于 indexes 列表的大小
        // toArray(new String[indexes.size()]) 使用提供的数组来构建一个新的数组，并将 List 中的元素拷贝到这个数组中
        // List 是动态的，可以随时增加或删除元素，而数组是静态的，一旦创建就不能更改大小
        // 所以，将 List 转换为数组（String[]）是一种将动态数据结构（List）转化为静态数据结构（数组）的方法
        // toArray(new T[0]) 是一种推荐的写法
        // 原来的写法 new T[indexes.size()] 是可以工作的，但实际上并没有太大的必要，因为 toArray() 方法内部已经会根据列表的大小动态调整数组的大小
        // 使用 new T[0] 的主要优点是代码简洁，并且没有多余的数组创建开销
        create.index = indexes.toArray(new String[0]);

        tokenizer.pop();
        if (!"".equals(tokenizer.peek()))  // SQL 语句里有错误的尾部
            throw Error.InvalidCommandException;
        return create;
    }

    /**
     * Drop SQL:
     * <p>
     * drop table <table name>
     */
    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName))
            throw Error.InvalidCommandException;
        tokenizer.pop();

        if (!"".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    /**
     * = | > | <
     */
    private static boolean isCmpOp(String op) {
        return ("=".equals(op) ||
                ">".equals(op) ||
                "<".equals(op));
    }

    /**
     * and | or
     */
    private static boolean isLogicOp(String op) {
        return ("and".equals(op) ||
                "or".equals(op));
    }

    /**
     * SingleExpression SQL:
     * <p>
     * <field name> (><=) <value>
     */
    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if (!isName(field))
            throw Error.InvalidCommandException;
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if (!isCmpOp(op))
            throw Error.InvalidCommandException;
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    /**
     * Where SQL:
     * <p>
     * where <field name> (><=) <value> [(and or) <field name> (><=) <value>]
     * <p>
     * eg:
     * <p>
     * where age > 10
     * <p>
     * where age > 10 or age < 3
     */
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if (!"where".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        tokenizer.pop();

        where.singleExp1 = parseSingleExp(tokenizer);

        String logicOp = tokenizer.peek();
        if ("".equals(logicOp)) {  // 当前已经是最后一个条件了
            where.logicOp = "";
            return where;
        }
        if (!isLogicOp(logicOp))
            throw Error.InvalidCommandException;
        where.logicOp = logicOp;
        tokenizer.pop();

        where.singleExp2 = parseSingleExp(tokenizer);

        if (!"".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        return where;
    }

    /**
     * Select SQL:
     * select (*<field name list>) from <table name> [<where statement>]
     * <p>
     * eg:
     * <p>
     * select * from student where id = 1
     * <p>
     * select name from student where id > 1 and id < 4
     * <p>
     * select name, age, id from student where id = 12
     */
    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();  // *
        if ("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while (true) {
                String field = tokenizer.peek();
                if (!isName(field))
                    throw Error.InvalidCommandException;
                fields.add(field);
                tokenizer.pop();
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else
                    break;
            }
        }
        read.field = fields.toArray(new String[0]);

        if (!"from".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName))
            throw Error.InvalidCommandException;
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    /**
     * Insert SQL:
     * <p>
     * insert into <table name> values <value list>
     * <p>
     * eg:
     * <p>
     * insert into student values 5 "XZ" 22
     */
    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if (!"into".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName))
            throw Error.InvalidCommandException;
        insert.tableName = tableName;
        tokenizer.pop();

        if (!"values".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;

        List<String> values = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if ("".equals(value))
                break;
            else
                values.add(value);
        }
        insert.values = values.toArray(new String[0]);
        return insert;
    }

    /**
     * Delete SQL:
     * <p>
     * delete from <table name> <where statement>
     * <p>
     * eg:
     * <p>
     * delete from student where name = "XZ"
     */
    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if (!"from".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName))
            throw Error.InvalidCommandException;
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    /**
     * Update SQL:
     * <p>
     * update <table name> set <field name>=<value> [<where statement>]
     * <p>
     * eg:
     * <p>
     * update student set name = "ZYJ" where id = 5
     */
    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();

        String tableName = tokenizer.peek();
        if (!isName(tableName))
            throw Error.InvalidCommandException;
        update.tableName = tableName;
        tokenizer.pop();

        if (!"set".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        tokenizer.pop();

        String fieldName = tokenizer.peek();
        if (!isName(fieldName))
            throw Error.InvalidCommandException;
        update.fieldName = fieldName;
        tokenizer.pop();

        if (!"=".equals(tokenizer.peek()))
            throw Error.InvalidCommandException;
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        if ("".equals(tokenizer.peek())) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    /**
     * Show SQL:
     * <p>
     * show
     */
    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        if ("".equals(tokenizer.peek()))
            return new Show();
        throw Error.InvalidCommandException;
    }
}
