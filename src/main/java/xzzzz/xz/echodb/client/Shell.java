package xzzzz.xz.echodb.client;

import java.util.Scanner;

/**
 * 用于接受用户的输入，并调用 Client.execute()
 */
public class Shell {

    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    public void run() {
        Scanner sc = new Scanner(System.in);  // 创建了一个 Scanner 对象，它会从标准输入（键盘）中读取内容
        try {
            while (true) {
                System.out.print(":> ");  // 打印提示符
                String statStr = sc.nextLine();  // 读取用户输入的一整行（直到换行）
                if ("exit".equals(statStr) || "quit".equals(statStr))
                    break;

                try {
                    byte[] res = client.execute(statStr.getBytes());  // 将用户的输入转换为字节数组，并执行
                    System.out.println(new String(res));  // 将执行结果转换为字符串，并打印
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } finally {
            sc.close();  // 关闭Scanner
            client.close();
        }
    }
}
