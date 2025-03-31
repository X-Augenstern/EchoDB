package xzzzz.xz.echodb.transport;

/**
 * 在 EchoDB 中传输数据使用了一种特殊的二进制格式，用于客户端和通信端之间的通信。在数据的传输和接受之前，会通过 Package 进行数据的加密以及解密：
 * <p>
 * - [Flag] [Data]
 * <p>
 * - 若 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身，err 就为空
 * <p>
 * - 若 flag 为 1，表示发送的是错误信息，那么 data 为空， err 为错误提示信息
 */
public class Package {

    /**
     * 存放数据信息
     */
    byte[] data;

    /**
     * 存放错误提示信息
     */
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
