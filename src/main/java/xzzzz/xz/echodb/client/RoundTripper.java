package xzzzz.xz.echodb.client;

import xzzzz.xz.echodb.transport.Package;
import xzzzz.xz.echodb.transport.Packager;

import java.io.IOException;

/**
 * 用于客户端发送请求并接受响应，实现了单次收发动作
 */
public class RoundTripper {

    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 用于处理请求的往返传输
     */
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);  // 发送请求包
        return packager.receive();  // 接收响应包，并返回
    }

    public void close() throws IOException {
        packager.close();
    }
}
