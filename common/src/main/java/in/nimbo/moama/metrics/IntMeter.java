package in.nimbo.moama.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public final class  IntMeter implements Metered {
    private static final String RATE_NUM_OF = "rate/num of ";
    private int value;
    private int lastValue;
    private Meter jmx;
    private String name;
    static MetricRegistry metrics = new MetricRegistry();

    public IntMeter(String name) {
        jmx = metrics.meter(name);
        this.name = name;
        Metrics.addMeter(this);
    }

    public void add(int value) {
        jmx.mark(value);
        this.value += value;
    }

    public void increment() {
        jmx.mark();
        value++;
    }

    public double rate(double delta) {
        return (value - lastValue) / delta;
    }

    public int getValue() {
        return value;
    }

    public String stat(double delta) {
        return RATE_NUM_OF + name + "\t" + 1.0* (value - lastValue) / delta + "\t" + (lastValue = value);
    }
}
