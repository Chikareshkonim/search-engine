package in.nimbo.moama.listener;

import org.junit.Test;

import static java.lang.Thread.sleep;

public class FunctionTest {


    @Test
    public void lastNews() throws InterruptedException {
        new Listener().listen(Function.class,4767);
        sleep(50000);
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