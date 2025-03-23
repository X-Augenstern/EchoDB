package xzzzz.xz.echodb.backend.dm.pageIndex;

import xzzzz.xz.echodb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 这个页面索引的设计用于提高在数据库中进行插入操作时的效率。它缓存了每一页的空闲空间信息，以便在进行插入操作时能够快速找到合适的页面，而无需遍历磁盘或者缓存中的所有页面。
 * <p>
 * 具体来说，页面索引根据页面容量划分为一定数量的区间（这里是40个区间），从小到大页面的空闲空间递增，并且在数据库启动时，会遍历所有页面，将每个页面的空闲空间信息分配到这些区间中。
 * 当需要进行插入操作时，插入操作首先会将所需的空间大小向上取整，然后映射到某一个区间，随后可以直接从该区间中选择任何一页，以满足插入需求。
 * <p>
 * PageIndex 的实现使用了一个数组，数组的每个元素都是一个列表，用于存储具有相同空闲空间大小的页面信息。
 * 从 PageIndex 中获取页面的过程非常简单，只需要根据所需的空间大小计算出区间号，然后直接从对应的列表中取出一个页面即可。
 * 被选择的页面会从 PageIndex 中移除，这意味着同一个页面不允许并发写入。在上层模块使用完页面后，需要将其重新插入 PageIndex，以便其他插入操作能够继续使用。
 * <p>
 * 总的来说，页面索引的设计旨在提高数据库的插入操作效率，通过缓存页面的空闲空间信息，避免了频繁地访问磁盘或者缓存中的页面，从而加速了插入操作的执行。
 */
public class PageIndex {

    /**
     * 将一页划分成40个区间
     */
    private final static int INTERVALS_NO = 40;

    private final static int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;

    private List<PageInfo>[] lists;

    public PageIndex() {
        this.lock = new ReentrantLock();
        this.lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 根据给定的页面编号和空闲空间大小添加一个 PageInfo 对象
     * 同一个页面是不允许并发写的，在上层模块使用完这个页面之后，需要重新将其插入到 PageIndex
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;  // 计算空闲空间大小对应的区间编号
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据给定的空间大小计算所处的编号位置，选择一个 PageInfo 对象，其空闲空间>=给定的空间大小。如果没有找到合适的 PageInfo，返回 null。
     */
    public PageInfo select(int spaceSize) {
        lock.lock();  // 获取锁，确保线程安全
        try {
            int number = spaceSize / THRESHOLD;  // 计算需要的空间大小对应的区间编号
            /*
                1、假如需要存储的字节大小为5168，此时计算出来的区间号是25，但是25*204=5100显然是不满足条件的
                2、此时向上取整找到26，而26*204=5304，是满足插入条件的
             */
            if (number < INTERVALS_NO) number++;  // 向上取整
            while (number <= INTERVALS_NO) {  // 从计算出的区间编号开始，向上寻找合适的 PageInfo
                if (lists[number].isEmpty()) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);  // 如果当前区间有 PageInfo，返回第一个 PageInfo，并从列表中移除
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
