package in.nimbo.moama;

import in.nimbo.moama.news.Queue;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class QueueTest {
    private Queue<Integer> queue = new Queue<>(10);

    @Before
    public void setUp() {
        queue.addUrls(Arrays.asList(1, 2, 3));
    }

    @Test
    public void getUrls() throws InterruptedException {
        int first = queue.getUrls().get(0);
        assertEquals(1, first);
    }

    @Test
    public void addUrls() {
        queue.addUrls(Arrays.asList(4, 5));
        assertEquals(5, queue.size());
    }
}