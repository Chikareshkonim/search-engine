package in.nimbo.moama.listener;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
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
        out.println("salam");
        Assert.assertEquals(scanner.nextLine(),"salam alaykom");
        Assert.assertEquals(scanner.nextLine(),"done");
        scanner.nextLine();
        out.println("salam");
        Assert.assertEquals(scanner.nextLine(),"salam alaykom");
        out.println("close");
        socket.getOutputStream();
    }

    static class ListenTestFunctionClass{

        public  static  void salam(PrintStream out, Scanner scanner){
            out.println("salam alaykom");
        }
    }
}