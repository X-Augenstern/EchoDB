package xzzzz.xz.echodb.backend.utils;

public class Panic {
    public static void panic(Exception e){
        e.printStackTrace();
        System.exit(1);  // 状态码 1：表示程序异常退出
    }
}
