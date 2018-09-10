package in.nimbo.moama;

import in.nimbo.moama.news.listener.Listener;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static java.lang.Thread.sleep;

public class ListenerFunctionTest {

    @Test
    public void exit() throws InterruptedException, IOException, URISyntaxException {
        Initializer.initialize();
        new Listener().listen(ListenerFunction.class,3333);
        sleep(30000);

    }
}