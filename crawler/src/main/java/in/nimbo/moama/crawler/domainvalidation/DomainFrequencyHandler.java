package in.nimbo.moama.crawler.domainvalidation;

import static in.nimbo.moama.util.Constants.CRAWLER_DOMAIN_CHECKER_HASH_PRIME;
import static in.nimbo.moama.util.Constants.CRAWLER_POLITE_TIME;

public class DomainFrequencyHandler {
    private static DomainFrequencyHandler ourInstance = new DomainFrequencyHandler();
    private static int domainHashPrime = 196613;

    public static DomainFrequencyHandler getInstance() {
        return ourInstance;
    }

    private long[] domainHashTableTime;

    private DomainFrequencyHandler() {
        domainHashPrime = CRAWLER_DOMAIN_CHECKER_HASH_PRIME;
        domainHashTableTime = new long[domainHashPrime];
    }

    public boolean isAllow(String url) {
        int hash = (url.hashCode() % domainHashPrime + domainHashPrime) % domainHashPrime;
        if (System.currentTimeMillis() - domainHashTableTime[hash] > CRAWLER_POLITE_TIME) {
            domainHashTableTime[hash] = System.currentTimeMillis();
            return true;
        }
        return false;
    }
}