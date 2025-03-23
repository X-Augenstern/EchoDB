package xzzzz.xz.echodb.backend.dm.pageIndex;

public class PageInfo {

    public int pgno;

    /**
     * 页面的空闲空间
     */
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
