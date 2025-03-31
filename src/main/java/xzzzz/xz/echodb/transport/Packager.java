package xzzzz.xz.echodb.transport;

import java.io.IOException;

/**
 * Encoder 和 Transporter 的结合体，直接对外提供 send 和 receive 方法
 * <p>
 * 发送->接收：Package -> byte[] -> 十六进制字符串 -> byte[] -> Package
 */
public class Packager {

    private Transporter transporter;

    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    /**
     * 发送数据
     */
    public void send(Package pkg) throws IOException {
        byte[] data = encoder.encode(pkg);
        transporter.send(data);
    }

    /**
     * 接收数据
     */
    public Package receive() throws Exception {
        byte[] data = transporter.receive();
        return encoder.decode(data);
    }

    public void close() throws IOException {
        transporter.close();
    }
}
