package in.nimbo.moama;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() throws InterruptedException {

        Thread thread=new Thread(()-> {throw new NullPointerException();});
        thread.start();
        sleep(1000);
        Assert.assertFalse(thread.isAlive());
    }
}
