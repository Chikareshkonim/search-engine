package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.HBaseManager;
import in.nimbo.moama.LRUCache;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.util.CrawlerPropertyType;

public class DuplicateHandler {
    private final LRUCache<String, Integer> lruCache;
    private HashDuplicateChecker HashDuplicateChecker;
    private static DuplicateHandler duplicateHandler = new DuplicateHandler();
    private static final int initialCapacity =Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.DUPLICATE_HANDLER_INITIAL_CAPACITY));
    private static final int maxCapacity =Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.DUPLICATE_HANDLER_MAX_CAPACITY));
    private static final DuplicateHandler ourInstance=new DuplicateHandler();
    private HBaseManager hBaseManager;

    private DuplicateHandler() {
        // FIXME: 8/28/18 ALIREZA
//        hBaseManager = new HBaseManager();
        lruCache = new LRUCache<>(initialCapacity, maxCapacity);
    }

    public static DuplicateHandler getInstance() {
        return ourInstance;
    }

    public boolean isDuplicate(String url) {
        //        } else if (hBaseManager.checkDuplicate(url)) {
//            lruCache.put(url, 0);
//            return true;
        return lruCache.containsKey(url);
    }
    public void weakConfirm(String url){
        lruCache.put(url,1);
    }

}
