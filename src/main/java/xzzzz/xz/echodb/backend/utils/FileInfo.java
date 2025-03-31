package xzzzz.xz.echodb.backend.utils;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 封装 fc + raf 的结构体
 */
public class FileInfo {

    private final RandomAccessFile raf;

    private final FileChannel fc;

    public FileInfo(FileChannel fc, RandomAccessFile raf) {
        this.raf = raf;
        this.fc = fc;
    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public FileChannel getFc() {
        return fc;
    }
}
