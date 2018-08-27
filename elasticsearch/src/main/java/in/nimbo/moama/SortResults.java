package in.nimbo.moama;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SortResults {
    public static Map<String, Float> sortByValues(Map<String, Float> map) {
        List<Map.Entry<String, Float>> list = new LinkedList<>(map.entrySet());
        list.sort(new CompareResults());
        Map<String, Float> sortedHashMap = new LinkedHashMap<>();
        for (Map.Entry<String, Float> entry : list) {
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }
        return sortedHashMap;

    }
}
