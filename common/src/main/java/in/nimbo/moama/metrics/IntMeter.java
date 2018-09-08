package in.nimbo.moama.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

public final class  IntMeter implements Metered {
    private static final String RATE_NUM_OF = "rate/num of ";
    private int value;
    private int lastValue;
    private Meter jmx;
    private String name;
    private static MetricRegistry metrics = new MetricRegistry();
    private static JmxReporter reporter = JmxReporter.forRegistry(metrics).build();

    static{
        reporter.start();
    }

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

    public float rate(float delta) {
        return (value - lastValue) / delta;
    }

    public int getValue() {
        return value;
    }

    public String stat(float delta) {
        return RATE_NUM_OF + name + "\t" + (double) (value - lastValue) / delta + "\t" + (lastValue = value);
    }
}
