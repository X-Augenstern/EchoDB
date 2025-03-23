package xzzzz.xz.echodb.commen;

public class Error {
    // Launcher
    public static final Exception InvalidMemException = new RuntimeException("Invalid memory!");

    // TM
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    // common
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");

    // DM
    public static Exception MemTooSmallException = new RuntimeException("Memory too small!");
    public static Exception FileExistsException = new RuntimeException("File already exists!");
    public static Exception FileCannotRWException = new RuntimeException("File cannot read or write!");
    public static Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static Exception BadLogFileException = new RuntimeException("Bad log file!");
    public static Exception PageIsNullException = new RuntimeException("Page is null!");
    public static Exception DataTooLargeException = new RuntimeException("Data too large!");
    public static Exception DatabaseBusyException = new RuntimeException("Database is busy!");
}
