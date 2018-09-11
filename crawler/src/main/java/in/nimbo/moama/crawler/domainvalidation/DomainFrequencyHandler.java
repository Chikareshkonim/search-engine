package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.elasticsearch.util.CrawlerPropertyType;

import java.util.List;
import java.util.stream.Collectors;

public class DomainFrequencyHandler {
    private static final DomainFrequencyHandler ourInstance=new DomainFrequencyHandler();
    private static int politeTime;
    private static int hashPrime;

    public static DomainFrequencyHandler getInstance() {
        return ourInstance;
    }

    private final long[] domainHashTableTime;

    private DomainFrequencyHandler() {

        politeTime = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_POLITE_TIME);
        hashPrime = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_DOMAIN_CHECKER_HASH_PRIME);
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
    public List<String> allowedLinks(List<String> documents) {
        return documents.stream().filter(this::isAllow).collect(Collectors.toList());
    }
}