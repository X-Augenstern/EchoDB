package xzzzz.xz.echodb.backend.server;

import xzzzz.xz.echodb.backend.parser.Parser;
import xzzzz.xz.echodb.backend.parser.statement.*;
import xzzzz.xz.echodb.backend.tbm.BeginRes;
import xzzzz.xz.echodb.backend.tbm.TableManager;
import xzzzz.xz.echodb.commen.Error;

/**
 * Executor对象，用于执行SQL语句
 */
public class Executor {

    /**
     * 当前事务id
     */
    private long xid;

    TableManager tbm;

    public Executor(TableManager tbm) {
        this.xid = 0;
        this.tbm = tbm;
    }

    /**
     * 事务id若不为0，异常退出并回滚
     */
    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    /**
     * 执行 SQL 语句
     * <p>
     * Begin：xid = 0 | Commit、Abort：xid != 0
     */
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);
        // instanceof 用于判断一个对象是不是某个类（或其子类、接口）的实例
        if (stat instanceof Begin) {
            if (xid != 0)
                throw Error.NestedTransactionException;
            BeginRes res = tbm.begin((Begin) stat);
            xid = res.xid;
            return res.result;
        } else if (stat instanceof Commit) {
            if (xid == 0)
                throw Error.NotInTransactionException;
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if (stat instanceof Abort) {
            if (xid == 0)
                throw Error.NotInTransactionException;
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else
            return execute2(stat);
    }

    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if (xid == 0) {  // 若当前没有开启新事务，开启一个新事务
            tmpTransaction = true;
            BeginRes res = tbm.begin(new Begin());
            xid = res.xid;
        }

        try {
            byte[] res = null;
            if (stat instanceof Show)
                res = tbm.show(xid);
            else if (stat instanceof Create)
                res = tbm.create(xid, (Create) stat);
            else if (stat instanceof Select)
                res = tbm.read(xid, (Select) stat);
            else if (stat instanceof Insert)
                res = tbm.insert(xid, (Insert) stat);
            else if (stat instanceof Delete)
                res = tbm.delete(xid, (Delete) stat);
            else if (stat instanceof Update)
                res = tbm.update(xid, (Update) stat);
            return res;
        } catch (Exception e1) {
            e = e1;
            throw e;
        } finally {
            if (tmpTransaction) {
                if (e != null)
                    tbm.abort(xid);
                else
                    tbm.commit(xid);
                xid = 0;
            }
        }
    }
}
