/*
 *@Type CmdClient.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:58
 * @version
 */
package client;

import java.util.Scanner;

public class CmdClient {
    private SocketClient socketClient;

    public CmdClient(SocketClient socketClient) {
        this.socketClient = socketClient;
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        String command;
        String key;
        String value;

        while (true) {
            System.out.print("Enter command (set, get, rm, quit): ");
            command = scanner.nextLine().trim();

            switch (command) {
                case "set":
                    System.out.print("Enter key: ");
                    key = scanner.nextLine().trim();
                    System.out.print("Enter value: ");
                    value = scanner.nextLine().trim();
                    socketClient.set(key, value);
                    break;
                case "get":
                    System.out.print("Enter key: ");
                    key = scanner.nextLine().trim();
                    socketClient.get(key);
                    break;
                case "rm":
                    System.out.print("Enter key: ");
                    key = scanner.nextLine().trim();
                    socketClient.rm(key);
                    break;
                case "quit":
                    System.out.println("Exiting CmdClient.");
                    scanner.close();
                    return;
                default:
                    System.out.println("Unknown command.");
            }
        }
    }

}