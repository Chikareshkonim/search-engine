package in.nimbo.moama.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;

public final class FloatMeter implements Metered{
    private static final String RATE_NUM_OF = "rate/num of ";
    private float value;
    private float lastValue;
    private String name;
    private Timer timer;
    private Timer.Context context;
    static MetricRegistry metrics = IntMeter.metrics;
    static JmxReporter reporter = JmxReporter.forRegistry(metrics).build();

    static{
        reporter.start();
    }


    public FloatMeter(String name) {
        timer = metrics.timer(name);
        this.name = name;
        Metrics.addMeter(this);
    }


    public void add(float value) {
        this.value += value;
    }

    public float rate(float delta) {
        return (value - lastValue) / delta;
    }

    public float getValue() {
        return value;
    }

    public String stat(float delta) {
        return RATE_NUM_OF + name + "\t" + (value - lastValue) / delta + "\t" + (lastValue = value);
    }
}