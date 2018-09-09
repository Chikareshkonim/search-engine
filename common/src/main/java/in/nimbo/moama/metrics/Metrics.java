package in.nimbo.moama.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Metrics {
    private static long lastTime = System.currentTimeMillis();
    private static List<Metered> meters =new ArrayList<>(30);

    public static void stat(Consumer<String> consumer) {
        double delta =  ((double)(System.currentTimeMillis() - lastTime) / 1000);
        meters.stream().map(e -> e.stat(delta)).forEach(consumer);
        lastTime = System.currentTimeMillis();
    }

    static void addMeter(Metered meter) {
        meters.add(meter);
    }
}