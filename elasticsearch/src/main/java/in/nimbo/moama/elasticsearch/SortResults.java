package in.nimbo.moama.elasticsearch;

import in.nimbo.moama.Tuple;

import java.util.*;

public class SortResults {
    public static Map<String, Float> sortByValues(Map<String, Float> map) {
        List<Map.Entry<String, Float>> list = new LinkedList<>(map.entrySet());
        list.sort(Comparator.comparing(Map.Entry::getValue));
        Collections.reverse(list);
        Map<String, Float> sortedHashMap = new LinkedHashMap<>();
        for (Map.Entry<String, Float> entry : list) {
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }
        return sortedHashMap;
    }

    public static Map<Tuple<String, Date>, Float> sortNews(Map<Tuple<String, Date>, Float> map) {
        List<Map.Entry<Tuple<String, Date>, Float>> list = new LinkedList<>(map.entrySet());
        list.sort(Comparator.comparing(Map.Entry::getValue));
        Collections.reverse(list);
        Map<Tuple<String, Date>, Float> sortedHashMap = new LinkedHashMap<>();
        for (Map.Entry<Tuple<String, Date>, Float> entry : list) {
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }
        return sortedHashMap;
    }


}
