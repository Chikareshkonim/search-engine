package in.nimbo.moama;

import java.util.LinkedHashMap;

public class RSSs {

    private static RSSs ourInstance = new RSSs();

    public static RSSs getInstance() {
        return ourInstance;
    }

    private RSSs() {
        loadRSSs();
    }

    private LinkedHashMap<String, String> rssToDomainMap;

    private LRUCache<String, Boolean> cache = new LRUCache<>(1000, 10000);

    public LinkedHashMap<String, String> getRssToDomainMap() {
        return rssToDomainMap;
    }

    public boolean isSeen (String url) {
        setSeen(url);
        return cache.containsKey(url);
    }

    private void setSeen(String url) {
        cache.put(url, true);
    }

    public void loadRSSs(){
        // TODO: 8/17/18
        rssToDomainMap = new LinkedHashMap<>();
        rssToDomainMap.put("http://www.asriran.com/fa/rss/1", "asriran.com");
    }
    public void saveRSSs(){
        // TODO: 8/17/18
    }
}
