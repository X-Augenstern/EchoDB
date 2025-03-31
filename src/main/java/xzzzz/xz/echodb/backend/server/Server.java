package xzzzz.xz.echodb.backend.server;

import xzzzz.xz.echodb.backend.tbm.TableManager;
import xzzzz.xz.echodb.transport.Encoder;
import xzzzz.xz.echodb.transport.Package;
import xzzzz.xz.echodb.transport.Packager;
import xzzzz.xz.echodb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Server 是一个服务器类，主要作用是监听指定的端口号，接受客户端的连接请求，并为每个连接请求创建一个新的线程来处理
 */
public class Server {

    private int port;

    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    /**
     * 开启服务器
     */
    public void start() {
        ServerSocket ss;  // 创建一个ServerSocket对象，用于监听指定的端口
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);

        // 创建一个线程池，用于管理处理客户端连接请求的线程
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while (true) {  // 无限循环，等待并处理客户端的连接请求
                Socket socket = ss.accept();  // 接收一个客户端的连接请求
                Runnable worker = new HandleSocket(socket, tbm);  // 创建一个新的HandleSocket对象，用于处理这个连接请求
                tpe.execute(worker);  // 将这个HandleSocket对象提交给线程池，由线程池中的一个线程来执行
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {
            }
        }
    }
}

/**
 * HandleSocket 类实现了 Runnable 接口，在建立连接后初始化 Packager，随后就循环接收来自客户端的数据并处理
 * 主要通过 Executor 对象来执行 SQL 语句，在接收、执行SQL语句的过程中发生异常的话，将会结束循环，并关闭 Executor 和 Transporter
 */
class HandleSocket implements Runnable {  // Runnable 接口是 Java 中实现多线程的一种方式，主要是为了让类具备“可以被线程执行”的能力

    private Socket socket;

    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {  // 只要实现了 run() 方法，就可以把对象交给 Thread 去运行
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();  // 获取远程客户端的地址信息
        System.out.println("Establishing connection: " + address.getAddress().getHostAddress() + ":" + address.getPort());  // 打印客户端的IP地址和端口号

        Packager packager;
        try {
            Transporter t = new Transporter(socket);  // 创建一个Transporter对象，用于处理网络传输
            Encoder e = new Encoder();  // 创建一个Encoder对象，用于处理数据的编码和解码
            packager = new Packager(t, e);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }

        Executor exe = new Executor(tbm);
        while (true) {
            Package pkg;
            try {
                pkg = packager.receive();  // 从客户端接收数据包
            } catch (Exception e) {
                break;
            }
            byte[] sql = pkg.getData();  // 获取数据包中的SQL语句
            byte[] res = null;
            Exception e = null;
            try {
                res = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }
            pkg = new Package(res, e);
            try {
                packager.send(pkg);  // 将数据包发回客户端
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

