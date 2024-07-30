/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package example;

import client.Client;
import client.CmdClient;
import client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        SocketClient socketClient = new SocketClient(host, port);
        CmdClient client=new CmdClient(socketClient);
        client.start();
//        client.get("zsy1");
//        client.set("zsy12","for test");
//        client.get("zsy12");
//        client.rm("zsy12");
//        client.get("zsy12");
    }
}