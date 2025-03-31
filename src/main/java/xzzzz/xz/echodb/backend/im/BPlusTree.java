package xzzzz.xz.echodb.backend.im;

import xzzzz.xz.echodb.backend.common.SubArray;
import xzzzz.xz.echodb.backend.dm.DataManager;
import xzzzz.xz.echodb.backend.dm.dataItem.DataItem;
import xzzzz.xz.echodb.backend.tm.TransactionManagerImpl;
import xzzzz.xz.echodb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * IM 对上层模块主要提供两种能力：
 * 插入索引和搜索节点
 * <p>
 * 这里可能会有疑问，IM 为什么不提供删除索引的能力。
 * 当上层模块通过 VM 删除某个 Entry，实际的操作是设置其 XMAX。
 * 如果不去删除对应索引的话，当后续再次尝试读取该 Entry 时，是可以通过索引寻找到的，但是由于设置了 XMAX，寻找不到合适的版本而返回一个找不到内容的错误。
 * <p>
 * 可能的错误与恢复：
 * <p>
 * B+ 树在操作过程中，可能出现两种错误，分别是节点内部错误和节点间关系错误。
 * <p>
 * 当节点内部错误发生时，即当 Ti 在对节点的数据进行更改时，MYDB 发生了崩溃。由于 IM 依赖于 DM，在数据库重启后，Ti 会被撤销（undo），对节点的错误影响会被消除。
 * <p>
 * 如果出现了节点间错误，那么一定是下面这种情况：某次对 u 节点的插入操作创建了新节点 v, 此时 sibling(u)=v，但是 v 却并没有被插入到父节点中。
 * [ parent ]
 * v
 * [u] -> [v]
 * <p>
 * 正确的状态应当如下：
 * [ parent ]
 * v      v
 * [u] -> [v]
 * 这时，如果要对节点进行插入或者搜索操作，如果失败，就会继续迭代它的兄弟节点，最终还是可以找到 v 节点。唯一的缺点仅仅是，无法直接通过父节点找到 v 了，只能间接地通过 u 获取到 v。
 */
public class BPlusTree {

    public DataManager dm;

    /**
     * rootUid被插入页面后返回的uid（这棵树的入口句柄）
     */
    public long bootUid;

    /**
     * 由于 B+ 树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个 bootDataItem，该 DataItem 中存储了根节点的 UID。
     * 可以注意到，IM 在操作 DM 时，使用的事务都是 SUPER_XID。
     */
    public DataItem bootDataItem;

    public Lock bootLock;

    /**
     * 创建一个空的根节点的字节表示，并返回这棵树的入口句柄（根节点的uid的uid：bootUid）
     * <p>
     * 如果以后想访问树的根节点
     * 就要先拿这个 long UID
     * 去数据文件里找这条 8 字节的记录（里面存的是 rootUid）
     * 然后根据 rootUid 再去找真正的根节点
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);  // 插入根节点数组得到根节点uid
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));  // 插入根节点的uid得到新的uid作为这棵树的入口句柄
        /*
            这是一种间接寻址机制，也叫“根指针的封装”，常用于以下目的：
            目的	                                说明
            ✅ 支持根节点动态变化	                未来根节点分裂后，指针会变，只需要更新“指向根节点的那条记录”，而不影响上层逻辑
            ✅ 分离“结构索引”和“结构入口”	        把根节点和根指针解耦，可以更安全/灵活地管理树结构
            ✅ 多棵树共存	                        每棵树都有自己的“根引用”，方便统一管理多个索引结构
         */
    }

    /**
     * 根据 bootUid 构建 B+树
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    /**
     * 获取 rootUid，bootDataItem 的 entry 部分的 [Xmin]：创建该条记录（版本）的事务编号
     */
    private long getRootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 创建一个新的根节点的原始字节数来更新 rootUid（entry 部分的 [Xmin]）
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 从当前节点递归查找并返回第一个比key大的叶子节点的 UID
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf)
            return nodeUid;
        else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 从当前节点递归查找第一个比目标 key 大的节点的 uid，找不到就去右兄弟继续，直到找到或走到链尾
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if (res.uid != 0)
                return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    /**
     * 在 B+树中搜索key为 leftKey 的所有子节点uid
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 在 B+树中搜索 leftKey - rightKey 范围内的所有子节点uid
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = getRootUid();
        long leafUid = searchLeaf(rootUid, leftKey);  // 找到第一个比leftKey大的叶子节点uid
        List<Long> uids = new ArrayList<>();
        while (true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);  // 在当前节点下搜索 leftKey - rightKey 范围内的所有子节点uid
            leaf.release();
            uids.addAll(res.uids);
            if (res.siblingUid == 0)  // rightKey 不是该节点的最大key
                break;
            else
                leafUid = res.siblingUid;  // rightKey 是该节点的最大 key，将 leafUid 更新为下一个兄弟节点的UID，继续遍历
        }
        return uids;
    }

    public static class InsertRes {
        long newNode, newKey;  // 如果不为 null，就是新节点的UID和新键
    }

    /**
     * 从当前节点递归插入（并在需要的时候分裂）新节点
     * <p>
     * 如果插入成功，结构内的 newNode、newKey 就看插入过程中有没有分裂过节点，分裂过就是新节点的，没有分裂过就是 null
     * 如果插入失败，就继续递归插入
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();

            if (iasr.siblingUid != 0)
                nodeUid = iasr.siblingUid;  // 节点插入失败，将 nodeUid 更新为下一个兄弟节点的UID，继续插入
            else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    /**
     * 从当前节点递归插入新节点，并在需要时处理节点分裂
     * <p>
     * 如果发生了分裂，在分裂后的新节点会被重新插入到父节点中。这是树结构（例如 B 树、B+ 树）中常见的插入分裂过程的一个关键部分。
     * 分裂后的新节点需要重新插入，这是为了保持树的平衡。新节点会被插入到分裂的父节点中，若父节点也满了，则会继续进行分裂，直到树的根节点。
     * <p>
     * 在分裂时，节点被拆分成两个部分，并且会生成一个新的节点，这个新节点需要被插入到父节点中。否则，树的结构就会不完整。具体流程如下：
     * <p>
     * 1. 插入并发生分裂：
     * 当一个节点（无论是叶节点还是非叶节点）由于容量问题无法继续插入时，它会发生分裂。
     * 分裂操作会将当前节点的部分数据（例如，某个键和与之关联的子节点）移到一个新的节点中。
     * 返回的分裂结果通常包含两个重要信息：新创建的子节点的UID和分裂出的“新键”（即分裂点的键值）。
     * <p>
     * 2. 新节点的重新插入：
     * 分裂后的新节点需要插入到父节点中，以保持树的平衡。否则，父节点会丢失对新节点的引用，导致树的结构不再有效。
     * 这时会进行递归插入操作：将分裂出来的新节点和新键插入到父节点中，可能会继续导致父节点的分裂，进而继续递归。
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res;
        if (isLeaf)
            // 如果当前节点是叶节点，则尝试将uid和key插入到该叶节点。如果该节点满了，会发生分裂
            res = insertAndSplit(nodeUid, uid, key);
        else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if (ir.newNode != 0)  // 该子节点进行了分裂，重新插入新节点
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            else
                res = new InsertRes();  // 如果没有发生分裂
        }
        return res;
    }

    /**
     * 往 B+ 树根节点递归插入新节点
     * <p>
     * 如果分裂一直“冒泡”到了根节点（即根节点也发生了分裂），那就需要创建一个新的根节点，将原来的根和新生成的子节点作为它的两个孩子
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = getRootUid();
        InsertRes res = insert(rootUid, uid, key);
        if (res.newNode != 0)
            updateRootUid(rootUid, res.newNode, res.newKey);
    }

    /**
     * 在使用完 bootDataItem 后，也应当及时调用 release() 方法，释放掉 DataItem 的缓存（由 DM 缓存 DataItem） 进而会释放包含的页面对象的缓存
     */
    public void close() {
        bootDataItem.release();
    }
}
