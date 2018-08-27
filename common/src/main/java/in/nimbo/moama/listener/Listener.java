package in.nimbo.moama.listener;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;

public class Listener {
    private static final int DEFAULT_LISTEN_PORT = 2719;
    private static final long NEXT_PORT_LISTEN_TIME_MILLISECONDS = 4000;
    private int listenPort = DEFAULT_LISTEN_PORT;
    private Class functionClass;
    private boolean isListening = true;

    private String findMethodName(String input) {
        String[] strings = input.toLowerCase().split("[ ]+");
        return strings[0] + Stream.of(strings).skip(1)
                .map(element -> element.substring(0, 1).toUpperCase().concat(element.substring(1)))
                .reduce(String::concat).get();
    }

    private void findAndCallMethod(PrintStream out, Scanner scanner) {
        String funcName = findMethodName(scanner.nextLine());
        if (funcName.equals("help")) {
            help(out, scanner);
            endMethod(out, scanner);
        } else {
            Method method = null;
            try {
                method = functionClass.getMethod(funcName, PrintStream.class, Scanner.class);
                method.invoke(null, out, scanner);
                endMethod(out, scanner);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace(out);
            }
        }
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
        out.println("enter method name/all to show you it's/their's help ");
        String methodName = scanner.nextLine();
        switch (methodName) {
            case "all":
                // FIXME: 8/18/18 
                break;
            default:
                // FIXME: 8/18/18 
                break;
        }
        for (Method method : functionClass.getMethods()) {
            out.print(method.getName());
            try {
                out.println("    :    " + method.getAnnotations()[0].annotationType().getField("help"));
            } catch (NoSuchFieldException e) {
                e.printStackTrace(out);
            }
        }
    }

    private void endMethod(PrintStream out, Scanner scanner) {
        out.println("done");
        if (scanner.next().equals("close")) {
            out.flush();
            out.close();
            scanner.close();
        } else {
            findAndCallMethod(out, scanner);
        }
    }

    private void run() {
        try (ServerSocket serverSocket = new ServerSocket(listenPort)) {
            while (isListening) {
                try (Socket socket = serverSocket.accept()) {
                    sleep(NEXT_PORT_LISTEN_TIME_MILLISECONDS);
                    Scanner scanner = new Scanner(socket.getInputStream());
                    PrintStream out = new PrintStream(socket.getOutputStream());
                    findAndCallMethod(out, scanner);
                } catch (IOException | InterruptedException ignored) {
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setListening(boolean listening) {
        isListening = listening;
    }
}