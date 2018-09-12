package in.nimbo.moama;

import org.junit.Assert;
import org.junit.Test;

public class LRUCacheTest {


    @Test
    public void test1() {
        LRUCache<Integer ,Integer> cache=new LRUCache<>(4,3);
        cache.put(5,0);
        cache.put(7,8);
        cache.put(6,5);
        cache.get(7);
        cache.put(5,4);
        Assert.assertEquals(cache.toString(),"{6=5, 7=8, 5=4}");
        Assert.assertEquals(4, (int) cache.get(5));
        Assert.assertEquals(cache.size(),3);
    }
}