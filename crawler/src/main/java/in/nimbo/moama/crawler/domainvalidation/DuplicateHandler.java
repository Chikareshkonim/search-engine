package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.HBaseManager;
import in.nimbo.moama.LRUCache;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.util.CrawlerPropertyType;

public class DuplicateHandler {
    private final LRUCache<String, Integer> lruCache;
    private HashDuplicateChecker HashDuplicateChecker;
    private static DuplicateHandler duplicateHandler = new DuplicateHandler();
    private static final int initialCapacity = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.DUPLICATE_HANDLER_INITIAL_CAPACITY));
    private static final int maxCapacity = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.DUPLICATE_HANDLER_MAX_CAPACITY));
    private static final DuplicateHandler ourInstance = new DuplicateHandler();
    private HBaseManager hBaseManager;
    private static IntMeter duplicateRejectByLru =new IntMeter("Duplicate Reject ByLru");
    private static IntMeter duplicateRejectByHBase=new IntMeter("Duplicate Reject ByHbase");
    private static IntMeter duplicateAccept=new IntMeter("Duplicate Accepted");

    private DuplicateHandler() {
        hBaseManager = new HBaseManager(ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_TABLE),
                ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_FAMILY_SCORE));
        lruCache = new LRUCache<>(initialCapacity, maxCapacity);
    }
    public static DuplicateHandler getInstance() {
        return ourInstance;
    }
    public boolean isDuplicate(String url) {
        if (lruCache.containsKey(url)) {
            System.out.println("true lru");
            duplicateRejectByLru.increment();
            return true;
        } else if (hBaseManager.isDuplicate(url)) {
            duplicateRejectByHBase.increment();
            lruCache.put(url, 0);
            return true;
        }
        duplicateAccept.increment();
        return false;
    }

    public void weakConfirm(String url) {
        lruCache.put(url, 1);
    }
}
