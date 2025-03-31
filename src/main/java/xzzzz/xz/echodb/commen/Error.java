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

    // VM
    public static Exception DeadlockException = new RuntimeException("Deadlock!");
    public static Exception NullEntryException = new RuntimeException("Null entry!");
    public static Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");

    // parser
    public static Exception InvalidCommandException = new RuntimeException("Invalid command!");
    public static Exception TableNoIndexException = new RuntimeException("Table has no index!");

    // TBM
    public static Exception InvalidFieldException = new RuntimeException("Invalid field type!");
    public static Exception FieldNotIndexedException = new RuntimeException("Field not indexed!");
    public static Exception FieldNotFoundException = new RuntimeException("Field not found!");
    public static Exception InvalidLogOpException = new RuntimeException("Invalid logic operation!");
    public static Exception InvalidValuesException = new RuntimeException("Invalid values!");
    public static Exception DuplicatedTableException = new RuntimeException("Duplicated table!");
    public static Exception TableNotFoundException = new RuntimeException("Table not found!");
}
