package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.HBaseManager;
import in.nimbo.moama.LRUCache;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.util.CrawlerPropertyType;

public class DuplicateHandler {
    private final LRUCache<String, Integer> lruCache;
    private static final int initialCapacity = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.DUPLICATE_HANDLER_INITIAL_CAPACITY);
    private static final int maxCapacity = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.DUPLICATE_HANDLER_MAX_CAPACITY);
    private static final DuplicateHandler ourInstance = new DuplicateHandler();
    private HBaseManager hBaseManager;
    private static final IntMeter DUPLICATE_LRU_HIT_METER =new IntMeter("Duplicate lru hit");
    private static final IntMeter DUPLICATE_HBASE_HIT_METER =new IntMeter("Duplicate hbase hit");

    private DuplicateHandler() {
        hBaseManager = new HBaseManager(ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_TABLE),
                ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_FAMILY_SCORE));
        lruCache = new LRUCache<>(initialCapacity, maxCapacity);
    }
    public static DuplicateHandler getInstance() {
        return ourInstance;
    }
    public boolean isDuplicate(String url) {
        if (lruCache.get(url)!=null) {
            DUPLICATE_LRU_HIT_METER.increment();
            return true;
        } else if (hBaseManager.isDuplicate(url)) {
            DUPLICATE_HBASE_HIT_METER.increment();
            lruCache.put(url, 0);
            return true;
        }
        return false;
    }
    public boolean weakCheckDuplicate(String url) {
        return lruCache.containsKey(url);
    }

    public void weakConfirm(String url) {
        lruCache.put(url, 1);
    }
}
