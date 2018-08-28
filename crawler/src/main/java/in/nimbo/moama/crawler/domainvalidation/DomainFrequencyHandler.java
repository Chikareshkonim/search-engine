package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.util.CrawlerPropertyType;
import org.apache.log4j.Logger;

public class DomainFrequencyHandler {
    private static Logger errorLogger = Logger.getLogger("error");
    private static DomainFrequencyHandler ourInstance;
    private static int politeTime;
    private static int hashPrime;

    public static DomainFrequencyHandler getInstance() {
        if(ourInstance == null){
            ourInstance = new DomainFrequencyHandler();
        }
        return ourInstance;
    }

    private long[] domainHashTableTime;

    private DomainFrequencyHandler() {

        politeTime = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_POLITE_TIME));
        hashPrime = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_DOMAIN_CHECKER_HASH_PRIME));
        domainHashTableTime = new long[hashPrime];
    }

    public boolean isAllow(String url) {
        int hash = (url.hashCode() % hashPrime + hashPrime) % hashPrime;
        if (System.currentTimeMillis() - domainHashTableTime[hash] > politeTime) {
            domainHashTableTime[hash] = System.currentTimeMillis();
            return true;
        }
        return false;
    }
}