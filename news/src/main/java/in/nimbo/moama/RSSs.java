package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;

import static in.nimbo.moama.newsutil.NewsPropertyType.*;


public class RSSs {
    private static final int INITIAL_CAPACITY = Integer.parseInt(ConfigManager.getInstance().getProperty(CACHE_INITIAL_CAPACITY));
    private static final int MAX_CAPACITY = Integer.parseInt(ConfigManager.getInstance().getProperty(CACHE_MAX_CAPACITY));
    private HBaseManager hBaseManager = new HBaseManager(ConfigManager.getInstance().getProperty(NEWS_PAGES_TABLE),
            ConfigManager.getInstance().getProperty(HBASE_VISITED_FAMILY));

    private static RSSs ourInstance = new RSSs();

    public static RSSs getInstance() {
        return ourInstance;
    }

    private RSSs() {
        loadRSSs();
    }

    private LinkedHashMap<String, String> rssToDomainMap = new LinkedHashMap<>();

    private LRUCache<String, Boolean> cache = new LRUCache<>(INITIAL_CAPACITY, MAX_CAPACITY);

    public LinkedHashMap<String, String> getRssToDomainMap() {
        return rssToDomainMap;
    }

    public boolean isSeen (String url) {
        boolean answer = cache.containsKey(url);
        if (!answer) {
            answer = hBaseManager.checkDuplicate(url);
            if (answer)
                setSeen(url);
        }
        return answer;
    }

    private void setSeen(String url) {
        cache.put(url, true);
    }

    public void loadRSSs(){
        // TODO: 8/17/18
        System.out.println("loading RSSs");
        ConfigManager configManager = ConfigManager.getInstance();
        NewsWebsiteHBaseManager websiteHBaseManager = new NewsWebsiteHBaseManager(configManager.getProperty(NEWS_WEBSITE_TABLE),
                configManager.getProperty(HBASE_TEMPLATE_FAMILY), configManager.getProperty(HBASE_RSS_FAMILY));
        List<JSONObject> rssList = websiteHBaseManager.getRSSList();
        rssList.forEach(json -> rssToDomainMap.put(json.getString("rss"), json.getString("domain")));
        System.out.println("RSSs loaded successfully");
    }

    public void saveRSSs(){
        // TODO: 8/17/18
    }
}
