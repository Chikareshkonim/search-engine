package in.nimbo.moama.metrics;

public final class  IntMeter implements Metered {
    private static final String RATE_NUM_OF = "rate/num of ";
    private int value;
    private int lastValue;
    private String name;

    public IntMeter(String name) {
        this.name = name;
        Metrics.addMeter(this);
    }

    public void add(int value) {
        this.value += value;
    }

    public void increment() {
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
