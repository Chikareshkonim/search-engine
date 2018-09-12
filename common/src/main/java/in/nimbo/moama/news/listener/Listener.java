package in.nimbo.moama.news.listener;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;

public class Listener {
    private static final int DEFAULT_LISTEN_PORT = 2719;
    private static final long NEXT_TIME_LISTEN_MILLISECONDS = 200;
    private int listenPort = DEFAULT_LISTEN_PORT;
    private Class functionClass;
    private boolean isListening = true;
    private String lastFunc;

    private String findMethodName(String input) {
        String[] strings = input.toLowerCase().split(" +");

        return strings[0] + Stream.of(strings).skip(1).map(element -> element.substring(0, 1).toUpperCase().concat(element.substring(1)))
                .reduce(String::concat).orElse("");
    }

    private void findAndCallMethod(PrintStream out, Scanner scanner) {
        String funcName = findMethodName(scanner.nextLine());
        if (funcName.equals("help")) {
            help(out, scanner);
            endRequest(out, scanner);
        } else {
            Method method;
            try {
                if (funcName.equals("close")) {
                    close(out, scanner);
                    return;
                } else if (funcName.equals("\u001B[a")) {
                    funcName = lastFunc;
                }
                method = functionClass.getMethod(funcName, PrintStream.class, Scanner.class);
                method.invoke(null, out, scanner);
                lastFunc = funcName;
                endRequest(out, scanner);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                out.println(e.getClass().getName());
                out.println("cause"+e.getCause());
                e.printStackTrace(out);
                endRequest(out, scanner);
            }
        }
    }

    private void close(PrintStream out, Scanner scanner) {
        out.flush();
        out.close();
        scanner.close();

    }

    public void listen(Class functionClass, int listenPort) {
        this.listenPort = listenPort;
        listen(functionClass);
    }

    public void listen(Class functionClass) {
        this.functionClass = functionClass;
        new Thread(this::run).start();
    }


    public void help(PrintStream out, Scanner scanner) {
        Arrays.stream(functionClass.getMethods())
                .filter(method -> method.getAnnotation(CLI.class) != null)
                .forEach(method -> out.printf("%-25s: %-10s\n", funcToOrder(method.getName()), method.getAnnotation(CLI.class).help()));
    }

    private String funcToOrder(String funcName) {
        return funcName.chars()
                .flatMap(e -> {
                    if (Character.isLowerCase(e)) return IntStream.of(e);
                    else return IntStream.of(' ', e);
                })
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString().toLowerCase();
    }

    private void endRequest(PrintStream out, Scanner scanner) {
        out.println("done");
        out.println("print your next order");
        findAndCallMethod(out, scanner);
    }

    private void run() {
        try (ServerSocket serverSocket = new ServerSocket(listenPort)) {
            while (isListening) {
                Socket socket;
                try {
                    socket = serverSocket.accept();
                    listenThread(socket);
                } catch (IOException | InterruptedException ignored) {
                }
            }
        } catch (IOException e) {
        }
    }

    private void listenThread(Socket socket) throws InterruptedException, IOException {
        new Thread(() -> {
            Scanner scanner;
            try {
                scanner = new Scanner(socket.getInputStream());
                PrintStream out = new PrintStream(socket.getOutputStream());
                findAndCallMethod(out, scanner);
                socket.close();
            } catch (IOException e) {
            }
        }).start();
        sleep(NEXT_TIME_LISTEN_MILLISECONDS);
    }
}