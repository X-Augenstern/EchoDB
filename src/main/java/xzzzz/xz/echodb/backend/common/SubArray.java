package xzzzz.xz.echodb.backend.common;

/**
 * 共享内存数组
 * <p>
 * SubArray 是一个原数组中某段的轻量引用，类似一个窗口、切片或视图，用来高效地访问和修改原始数组的某一段，而不需要复制数据。
 */
public class SubArray {

    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
