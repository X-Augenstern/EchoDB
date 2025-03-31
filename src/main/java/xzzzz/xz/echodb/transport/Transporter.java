package xzzzz.xz.echodb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * 编码之后的信息会通过 Transporter 类，写入输出流发送出去
 * <p>
 * 用于将数据加密成十六进制数据，这样可以避免特殊字符造成的问题，并在信息末尾加上换行符。这样在发送和接受数据时，可以简单使用 BufferedReader 和 BufferedWrite 进行读写数据
 */
public class Transporter {

    private Socket socket;

    private BufferedReader reader;

    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 将字节数组转换为十六进制字符串
     * <p>
     * eg:
     * <p>
     * byte[] data = new byte[] {0x01, 0x2A};
     * String hex = hexEncode(data); // 返回 "012a\n"
     */
    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true) + "\n";  // 使用小写字母（例如 0a1b2c 而不是 0A1B2C）
    }

    /**
     * 将十六进制字符串转换回字节数组
     * <p>
     * eg:
     * <p>
     * String hex = "012a";
     * byte[] bytes = hexDecode(hex); // 返回 new byte[]{0x01, 0x2A}
     */
    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }

    /**
     * 发送数据
     */
    public void send(byte[] data) throws IOException {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    /**
     * 接收数据
     */
    public byte[] receive() throws IOException, DecoderException {
        String line = reader.readLine();
        if (line == null)
            close();
        return hexDecode(line);
    }
}
