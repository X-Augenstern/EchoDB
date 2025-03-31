package xzzzz.xz.echodb.transport;

import com.google.common.primitives.Bytes;
import xzzzz.xz.echodb.commen.Error;

import java.util.Arrays;

/**
 * 将Package对象编码为字节数组 + 将字节数组解码为Package对象
 */
public class Encoder {

    /**
     * 将Package对象编码为字节数组。
     * <p>
     * 如果Package对象中的错误信息不为空，将错误信息编码为字节数组，并在字节数组前添加一个字节1。
     * <p>
     * 如果Package对象中的错误信息为空，将数据编码为字节数组，并在字节数组前添加一个字节0。
     */
    public byte[] encode(Package pkg) {
        if (pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if (err.getMessage() != null)
                msg = err.getMessage();
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else
            return Bytes.concat(new byte[]{0}, pkg.getData());
    }

    /**
     * 将字节数组解码为Package对象。
     * <p>
     * 如果字节数组的长度小于1，抛出InvalidPkgDataException异常。
     * <p>
     * 如果字节数组的第一个字节为0，将字节数组的剩余部分解码为数据，创建一个新的Package对象，其中数据为解码后的数据，错误信息为null。
     * <p>
     * 如果字节数组的第一个字节为1，将字节数组的剩余部分解码为错误信息，创建一个新的Package对象，其中数据为null，错误信息为解码后的错误信息。
     * <p>
     * 如果字节数组的第一个字节既不是0也不是1，抛出InvalidPkgDataException异常。
     */
    public Package decode(byte[] data) throws Exception {
        if (data.length < 1)
            throw Error.InvalidPkgDataException;
        if (data[0] == 0)
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        else if (data[0] == 1)
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        else
            throw Error.InvalidValuesException;
    }
}
