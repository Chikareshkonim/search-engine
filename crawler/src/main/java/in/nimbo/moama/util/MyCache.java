package in.nimbo.moama.util;
import java.util.LinkedHashMap;
import java.util.Map;

public class MyCache<T, G> extends LinkedHashMap<T, G> {
    private final int maxCapacity;

    public MyCache(int initialCapacity, int maxCapacity) {
        super(initialCapacity, 0.75f, true);
        this.maxCapacity = maxCapacity;
    }


    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return maxCapacity < size();
    }
}
