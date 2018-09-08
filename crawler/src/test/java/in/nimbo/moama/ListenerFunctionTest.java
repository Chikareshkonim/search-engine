package in.nimbo.moama;

import in.nimbo.moama.listener.Listener;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

public class ListenerFunctionTest {

    @Test
    public void exit() throws InterruptedException, IOException, URISyntaxException {
        Initializer.initialize();
        new Listener().listen(ListenerFunction.class,3333);
        sleep(30000);

    }
}