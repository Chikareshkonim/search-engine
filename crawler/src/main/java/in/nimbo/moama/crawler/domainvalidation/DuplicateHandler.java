package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.HBaseManager;
import in.nimbo.moama.LRUCache;

public class DuplicateHandler {
    private static DuplicateHandler ourInstance=new DuplicateHandler();
    private LRUCache<String, Integer> lruCache;
    private HashDuplicateChecker HashDuplicateChecker;
    private static DuplicateHandler duplicateHandler = new DuplicateHandler();
    private static final int INITIAL_CAPACITY = 0;
    private static final int MAX_CAPACITY = 0;
    private HBaseManager hBaseManager;

    private DuplicateHandler() {
        // FIXME: 8/28/18 ALIREZA
//        hBaseManager = new HBaseManager();
        lruCache = new LRUCache<>(INITIAL_CAPACITY, MAX_CAPACITY);
    }

    public static DuplicateHandler getInstance() {
        return ourInstance;
    }

    public boolean isDuplicate(String url) {
        if (lruCache.containsKey(url)) {
            return true;
        } else if (hBaseManager.checkDuplicate(url)) {
            lruCache.put(url, 0);
            return true;
        } else
            return false;
    }

}
