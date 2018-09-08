package in.nimbo.moama.listener;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.Scanner;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

public class ListenerTest {


    @Test
    public void listen() throws IOException, InterruptedException {
        new Listener().listen(ListenTestFunctionClass.class,6798);
        Socket socket = new Socket("localhost",6798);
        Scanner scanner = new Scanner(socket.getInputStream());
        PrintStream out=  new PrintStream(socket.getOutputStream());
        out.println("salam khobi chetori");

        Assert.assertEquals(scanner.nextLine(),"salam alaykom");
        Assert.assertEquals(scanner.nextLine(),"done");
        scanner.nextLine();
        out.println("salam khobi chetori");
        Assert.assertEquals(scanner.nextLine(),"salam alaykom");
        scanner.nextLine();
        scanner.nextLine();
        out.println("help");
        Assert.assertEquals("salam khobi chetori      : 100 ta salam",scanner.nextLine());
        out.println("close");
        socket.getOutputStream();
    }

    @Test
    public void findMethodName() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method=Listener.class.getDeclaredMethod("findMethodName", String.class);
        method.setAccessible(true);
        Assert.assertEquals(method.invoke(new Listener(),"salam khobi"),"salamKhobi");
    }

    @Test
    public void funcToOrder() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method=Listener.class.getDeclaredMethod("funcToOrder",String.class);
        method.setAccessible(true);
        Assert.assertEquals(method.invoke(new Listener(),"salamKhobi"),"salam khobi");
    }


    static class ListenTestFunctionClass{
        @CLI(help = "100 ta salam")
        public  static  void salamKhobiChetori(PrintStream out, Scanner scanner){
            out.println("salam alaykom");
        }
    }
}