package in.nimbo.moama.news;

import in.nimbo.moama.HBaseManager;
import in.nimbo.moama.LRUCache;
import in.nimbo.moama.NewsWebsiteHBaseManager;
import in.nimbo.moama.configmanager.ConfigManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;

import static in.nimbo.moama.news.newsutil.NewsPropertyType.*;


public class RSSs {
    private static final int INITIAL_CAPACITY = ConfigManager.getInstance().getIntProperty(CACHE_INITIAL_CAPACITY);
    private static final int MAX_CAPACITY = ConfigManager.getInstance().getIntProperty(CACHE_MAX_CAPACITY);
    private static final int INITIAL_POLITE_CAPACITY = 1000;
    private static final int MAX_POLITE_CAPACITY = 100;
    private static final long POLITE_WAIT_TIME = ConfigManager.getInstance().getLongProperty(NEWS_POLITENESS_WAIT);
    private HBaseManager hBaseManager = new HBaseManager(ConfigManager.getInstance().getProperty(NEWS_PAGES_TABLE),
            ConfigManager.getInstance().getProperty(HBASE_VISITED_FAMILY));
    private static final Logger LOGGER = Logger.getRootLogger();


    private static RSSs ourInstance = new RSSs();

    public static RSSs getInstance() {
        return ourInstance;
    }

    private RSSs() {
        loadRSSs();
    }

    private LinkedHashMap<String, String> rssToDomainMap = new LinkedHashMap<>();

    private LRUCache<String, Boolean> cache = new LRUCache<>(INITIAL_CAPACITY, MAX_CAPACITY);
    private LRUCache<String, Long> politeCache = new LRUCache<>(INITIAL_POLITE_CAPACITY, MAX_POLITE_CAPACITY);

    public LinkedHashMap<String, String> getRssToDomainMap() {
        return rssToDomainMap;
    }

    public boolean isSeen(String url) {
        boolean answer = cache.get(url) != null;
        if (!answer) {
            answer = hBaseManager.isDuplicate(url);
            if (answer)
                setSeen(url);
        }
        return answer;
    }

    public boolean isPolite(String domain) {
        Long time = politeCache.get(domain);
        if (time == null || System.currentTimeMillis() - time > POLITE_WAIT_TIME) {
            politeCache.put(domain, System.currentTimeMillis());
            return true;
        }
        return false;
    }

    private void setSeen(String url) {
        cache.put(url, true);
    }

    public void loadRSSs(){
        LOGGER.info("loading rss...");
        ConfigManager configManager = ConfigManager.getInstance();
        NewsWebsiteHBaseManager websiteHBaseManager = new NewsWebsiteHBaseManager(configManager.getProperty(NEWS_WEBSITE_TABLE),
                configManager.getProperty(HBASE_TEMPLATE_FAMILY), configManager.getProperty(HBASE_RSS_FAMILY));
        List<JSONObject> rssList = websiteHBaseManager.getRSSList();
        rssList.forEach(json -> rssToDomainMap.put(json.getString("rss"), json.getString("domain")));
        LOGGER.info("rss loaded successfully");
    }
}
