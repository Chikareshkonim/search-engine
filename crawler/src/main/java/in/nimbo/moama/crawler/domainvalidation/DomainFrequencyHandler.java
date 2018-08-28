package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.util.PropertyType;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;

public class DomainFrequencyHandler {
    private static Logger errorLogger = Logger.getLogger("error");
    private static DomainFrequencyHandler ourInstance;
    private static int politeTime;
    private static int hashPrime;
    private ConfigManager configManager;

    public static DomainFrequencyHandler getInstance() {
        if(ourInstance == null){
            ourInstance = new DomainFrequencyHandler();
        }
        return ourInstance;
    }

    private long[] domainHashTableTime;

    private DomainFrequencyHandler() {
        try {
            configManager = new ConfigManager(new File(getClass().getClassLoader().getResource("config.properties").getFile()).getAbsolutePath(), PROPERTIES);
        } catch (IOException e) {
            errorLogger.error("Loading properties failed");
        }
        politeTime = Integer.parseInt(configManager.getProperty(PropertyType.CRAWLER_POLITE_TIME));
        hashPrime = Integer.parseInt(configManager.getProperty(PropertyType.CRAWLER_DOMAIN_CHECKER_HASH_PRIME));
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