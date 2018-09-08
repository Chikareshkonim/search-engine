package in.nimbo.moama.metrics;

public final class FloatMeter implements Metered{
    private static final String RATE_NUM_OF = "rate/num of ";
    private float value;
    private float lastValue;
    private String name;

    public FloatMeter(String name) {
        this.name = name;
        Metrics.addMeter(this);
    }

    public void add(float value) {

        this.value += value;
    }

    public void increment() {
        value++;
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