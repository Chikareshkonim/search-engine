package in.nimbo.moama;

import newsutil.NewsConfigManager;

import java.util.LinkedHashMap;

import static newsutil.NewsPropertyType.CACHE_INITIAL_CAPACITY;
import static newsutil.NewsPropertyType.CACHE_MAX_CAPACITY;


public class RSSs {
    private static final int INITIAL_CAPACITY = Integer.parseInt(NewsConfigManager.getInstance()
            .getProperty(CACHE_INITIAL_CAPACITY));
    private static final int MAX_CAPACITY = Integer.parseInt(NewsConfigManager.getInstance()
            .getProperty(CACHE_MAX_CAPACITY));

    private static RSSs ourInstance = new RSSs();

    public static RSSs getInstance() {
        return ourInstance;
    }

    private RSSs() {
        loadRSSs();
    }

    private LinkedHashMap<String, String> rssToDomainMap;

    private LRUCache<String, Boolean> cache = new LRUCache<>(INITIAL_CAPACITY, MAX_CAPACITY);

    public LinkedHashMap<String, String> getRssToDomainMap() {
        return rssToDomainMap;
    }

    public boolean isSeen (String url) {
        boolean answer = cache.containsKey(url);
        if (!answer)
            setSeen(url);
        return answer;
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
