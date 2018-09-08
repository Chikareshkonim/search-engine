package in.nimbo.moama.metrics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FloatMeterTest {    FloatMeter floatMeter;

    @Before
    public void setUp() {
        floatMeter = new FloatMeter("salam");
        floatMeter.add(1000);
        floatMeter.stat(100);
    }

    @Test
    public void add() {
        floatMeter.add(444);
        Assert.assertEquals(floatMeter.getValue(), 1444,0.0001);
    }

    @Test
    public void increment() {
        floatMeter.add(2);
        Assert.assertEquals(floatMeter.getValue(), 1002,0.001);
    }

    @Test
    public void rate() {
        floatMeter.add(1010);
        Assert.assertEquals(floatMeter.rate(10),101.0,0.01);
    }


    @Test
    public void stat() {
        floatMeter.add(2330);
        Assert.assertEquals(floatMeter.stat(10),"rate/num of salam\t233.0\t3330.0");
    }

}