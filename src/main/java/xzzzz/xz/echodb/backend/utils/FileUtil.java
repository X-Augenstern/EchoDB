package xzzzz.xz.echodb.backend.utils;

import xzzzz.xz.echodb.commen.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class FileUtil {

    public enum Mode {CREATE, OPEN}

    /**
     * 检查 CREATE / OPEN 模式下文件是否存在、是否可读写
     */
    public static void checkFile(File f, Mode mode) {
        try {
            if (mode == Mode.CREATE) {
                if (!f.createNewFile())
                    Panic.panic(Error.FileExistsException);
            } else {
                if (!f.exists())
                    Panic.panic(Error.FileNotExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite())
            Panic.panic(Error.FileCannotRWException);
    }

    /**
     * 检查 CREATE / OPEN 模式下文件是否存在、是否可读写，并返回 raf、fc
     */
    public static FileInfo checkFileAndBuildInfo(File f, Mode mode) {
        checkFile(f, mode);

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new FileInfo(fc, raf);
    }
}
