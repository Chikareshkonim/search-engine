package in.nimbo.moama.crawler.domainvalidation;

public class DomainFrequencyHandler {
    public static final int PLOITE_TIME = 28000;
    private static DomainFrequencyHandler ourInstance = new DomainFrequencyHandler();
    private static int domainHashPrime = 196613;

    public static DomainFrequencyHandler getInstance() {
        return ourInstance;
    }

    private long[] domainHashTableTime;

    private DomainFrequencyHandler() {
        domainHashPrime = 786433;
        domainHashTableTime = new long[domainHashPrime];
    }

    public boolean isAllow(String url) {
        int hash = (url.hashCode() % domainHashPrime + domainHashPrime) % domainHashPrime;
        if (System.currentTimeMillis() - domainHashTableTime[hash] > PLOITE_TIME) {
            domainHashTableTime[hash] = System.currentTimeMillis();
            return true;
        }
        return false;
    }
}