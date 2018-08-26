package in.nimbo.moama;

import in.nimbo.moama.crawler.domainvalidation.DuplicateLinkHandler;
import in.nimbo.moama.metrics.Metrics;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import static java.lang.Thread.sleep;

public class Listener {
    private static final int LISTEN_PORT = 2719;

    public static void main(String[] args) {
        listen();
    }

    public static void listen() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(LISTEN_PORT)) {
                while (true) {
                    try (Socket socket = serverSocket.accept()) {
                        sleep(4000);
                        Scanner scanner = new Scanner(socket.getInputStream());
                        String[] strings = scanner.nextLine().split("[ ]+");
                        String funcName = "";
                        for (String string : strings) {
                            funcName = funcName.concat(string.substring(0, 1).toUpperCase()).concat(string.substring(1).toLowerCase());
                        }
                        funcName = funcName.substring(0, 1).toLowerCase().concat(funcName.substring(1));
                        Method method = Listener.class.getMethod(funcName, PrintStream.class, Scanner.class);
                        method.invoke(null, new PrintStream(socket.getOutputStream()), scanner);
                    } catch (IOException | InterruptedException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public static void loadDuplicate(PrintStream out, Scanner scanner) {
        out.println("load duplicate called");
        try {
            DuplicateLinkHandler.getInstance().loadHashTable();
            out.println("done");
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace(out);
            out.flush();
            out.close();

        }

    }

    public static void saveDuplicate(PrintStream out, Scanner scanner) {
        out.println("save duplicate called");
        DuplicateLinkHandler.getInstance().saveHashTable();
        out.println("done");
        out.flush();
        out.close();
    }

    public static void stat(PrintStream out, Scanner scanner) {
        Metrics.stat(out);
        out.flush();
        out.close();
    }

    public static void exit(PrintStream out, Scanner scanner) {
        saveDuplicate(out, scanner);
        System.exit(0);
    }

    public static void isDuplicatte(PrintStream out, Scanner scanner) {
        out.println(DuplicateLinkHandler.getInstance().isDuplicate(scanner.nextLine()));
    }
}

