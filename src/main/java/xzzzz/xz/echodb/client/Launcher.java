package xzzzz.xz.echodb.client;

import xzzzz.xz.echodb.transport.Encoder;
import xzzzz.xz.echodb.transport.Packager;
import xzzzz.xz.echodb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

/**
 * 启动客户端并连接服务器
 */
public class Launcher {

    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
        System.out.println("Connected to database at 127.0.0.1:9999");  // 确认连接成功
    }
}
