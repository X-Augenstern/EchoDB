package xzzzz.xz.echodb.backend.common;

import xzzzz.xz.echodb.commen.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
  比较点	                抽象方法	                    接口方法
  所在位置	            抽象类 (abstract class)	    接口 (interface)
  默认修饰符	            protected / public	        public abstract（默认）
  是否可以多继承	        ❌ 不能	                    ✅ 可以
  是否可有变量	        ✅ 可以有成员变量	            ✅ 只能是 public static final
  实现方式	            被子类 extends 并 override	被类 implements 并 override
  是否可以有构造函数	    ✅ 可以	                    ❌ 不可以（接口不能实例化）
  是否可以包含非抽象方法	✅ 可以有普通方法（有方法体）	✅ Java 8+ 允许有 default 和 static 方法
  是否有方法体	        ❌ 没有方法体	                ❌ 没有方法体（除非用 default 或 static）
 */

/**
 * AbstractCache 实现了一个引用计数策略的缓存框架
 * <p>
 * 引用计数缓存框架是一种通用的缓存策略，与LRU（最近最少使用）相比，它采用了不同的资源管理方式。
 * 在引用计数缓存框架中，缓存的释放是由上层模块主动调用释放方法来触发的，而不是被动地由缓存管理器自动驱逐。
 * 当某个资源不再被上层模块引用时，通过调用释放方法来释放对该资源的引用。只有当资源的引用计数归零时，缓存才会驱逐该资源。
 * 这种方式可以确保缓存中的资源只有在确实不再被使用时才会被释放，避免了不必要的资源驱逐和回源操作。
 */
public abstract class AbstractCache<T> {

    /**
     * 用于存储实际缓存的数据
     * 键是资源的唯一标识符（通常是资源的ID或哈希值），值是缓存的资源对象（类型为 T）。在这个缓存框架中，cache 承担了普通缓存功能，即存储实际的资源数据
     */
    private HashMap<Long, T> cache;

    /**
     * 用于记录每个资源的引用个数
     * 键是资源的唯一标识符，值是一个整数，表示该资源当前的引用计数。引用计数表示有多少个模块或线程正在使用特定的资源。通过跟踪引用计数，可以确定何时可以安全地释放资源
     */
    private HashMap<Long, Integer> references;

    /**
     * 用于记录哪些资源当前正在从数据源获取中
     * 键是资源的唯一标识符，值是一个布尔值，表示该资源是否正在被获取中
     * 在多线程环境下，当某个线程尝试从数据源获取资源时，需要标记该资源正在被获取，以避免其他线程重复获取相同的资源。getting 映射用于处理多线程场景下的并发访问问题
     */
    private HashMap<Long, Boolean> getting;

    /**
     * 最大缓存资源数
     */
    private int maxResource;

    /**
     * 缓存中元素个数
     */
    private int count;

    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        this.cache = new HashMap<>();
        this.references = new HashMap<>();
        this.getting = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    /**
     * Page - key: pgno
     * DataItem - key: uid
     */
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {  // 有其它线程正在获取相同资源
                lock.unlock();
                try {
                    Thread.sleep(1);  // 释放锁，等待一段时间重新滚获取资源
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {  // 资源已经在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            if (maxResource > 0 && count == maxResource) {  // 缓存已满
                lock.unlock();
                throw Error.CacheFullException;
            }

            count++;
            getting.put(key, true);  // 该线程准备从数据源获取资源了
            lock.unlock();
            break;
        }

        // 尝试获取资源
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {  // 获取资源失败
            lock.lock();
            count--;  // 回退缓存元素个数
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        // 获取资源成功
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为（释放缓存）
     */
    protected abstract void releaseForCache(T obj);

    /**
     * 在上层模块不使用某个资源时，释放对资源的引用。当引用归零时，缓存就会驱逐这个资源
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {  // 资源当前的引用计数为0，说明没有别的模块或线程正在使用了，从缓存中驱逐
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else  // 还有别的正在使用，引用计数-1，把资源保留在缓存内
                references.put(key, ref);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }
}
