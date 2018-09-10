package in.nimbo.moama.news.listener;

import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.util.Formatter;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;

public class FunctionTest {


    @Test
    public void lastNews() throws InterruptedException, IOException {
        new Listener().listen(Function.class,4767);
        Socket socket = new Socket("localhost", 4767);
        Formatter formatter = new Formatter(socket.getOutputStream());
        Scanner scanner = new Scanner(socket.getInputStream());
        formatter.format("check\n");
        formatter.flush();
        assertEquals("You are connected", scanner.nextLine());
        formatter.close();
        scanner.close();
        socket.close();
    }

    @Test
    public void newsTrendWordInLastHour() {
    }

    @Test
    public void newsTrendWordInLastDay() {
    }

    @Test
    public void newsTrendWordInLastWeek() {
    }

    @Test
    public void addRss() {
    }

    @Test
    public void showTemplates() {
    }

    @Test
    public void showRss() {
    }

    @Test
    public void deleteTemplate() {
    }

    @Test
    public void deleteRss() {
    }

    @Test
    public void saveAll() {
    }

    @Test
    public void loadAll() {
    }

    @Test
    public void exit() {
    }
}