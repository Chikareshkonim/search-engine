package in.nimbo.moama.metrics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IntMeterTest {
    IntMeter intMeter;

    @Before
    public void setUp() {
        intMeter = new IntMeter("salam");
        intMeter.add(1000);
        intMeter.stat(100);
    }

    @Test
    public void add() {
        intMeter.add(444);
        Assert.assertEquals(intMeter.getValue(), 1444);
    }

    @Test
    public void increment() {
        intMeter.increment();
        intMeter.increment();
        Assert.assertEquals(intMeter.getValue(), 1002);
    }

    @Test
    public void rate() {
        intMeter.add(1010);
        Assert.assertEquals(intMeter.rate(10),101.0,0.01);
    }


    @Test
    public void stat() {
        intMeter.add(2330);
        Assert.assertEquals(intMeter.stat(10),"rate/num of salam\t233.0\t3330");
    }
}