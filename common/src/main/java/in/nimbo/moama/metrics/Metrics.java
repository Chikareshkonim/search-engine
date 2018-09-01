package in.nimbo.moama.metrics;

import org.apache.log4j.Logger;

import java.io.PrintStream;

public class Metrics {
    private static final String NUM_RATE_OF = "num/rate of ";
    private static final short B_TO_MB_BIT_SHIFT = 20;

    public static int numberOfUrlReceived = 0;
    public static int numberOfNull = 0;
    public static int numberOfDuplicate = 0;
    public static int numberOfDomainError = 0;
    public static long byteCounter = 0;
    public static int numberOFCrawledPage = 0;
    public static int numberOfLanguagePassed = 0;
    public static int numberOfPagesAddedToElastic = 0;
    public static int numberOfPagesAddedToHBase = 0;
    public static int numberOFComplete=0;

    private static long lastTime = System.currentTimeMillis();
    private static final Logger infoLogger = Logger.getLogger("info");
    private static int lastNumberOfLanguagePassed = 0;
    private static int lastNumberOfCrawledPage = 0;
    private static int lastNumberOfDuplicate = 0;
    private static int lastNumberOfDomainError = 0;
    private static int lastNumberOfUrlReceived = 0;
    private static int lastNumberOfPagesAddedToElastic = 0;
    private static int lastNumberOfPagesAddedToHBase = 0;

    public static void loadStat() {
    }

    public static void logStat() {
        int delta = (int) ((System.currentTimeMillis() - lastTime) / 1000);
        infoLogger.info("received MB     " + (byteCounter >> B_TO_MB_BIT_SHIFT));
        infoLogger.info(NUM_RATE_OF + "received url    " + numberOfUrlReceived + "\t" + (double) (numberOfUrlReceived - lastNumberOfUrlReceived) / delta);
        infoLogger.info(NUM_RATE_OF + "passed lang     " + numberOfLanguagePassed + "\t" + (double) (numberOfLanguagePassed - lastNumberOfLanguagePassed) / delta);
        infoLogger.info(NUM_RATE_OF + "duplicate       " + numberOfDuplicate + "\t" + (double) (numberOfDuplicate - lastNumberOfDuplicate) / delta);
        infoLogger.info(NUM_RATE_OF + "domain Error    " + numberOfDomainError + "\t" + (double) (numberOfDomainError - lastNumberOfDomainError) / delta);
        infoLogger.info(NUM_RATE_OF + "crawl           " + numberOFCrawledPage + "\t" + (double) (numberOFCrawledPage - lastNumberOfCrawledPage) / delta);
        infoLogger.info(NUM_RATE_OF + "HBase           " + numberOfPagesAddedToHBase + "\t" + (double) (numberOfPagesAddedToHBase - lastNumberOfPagesAddedToHBase) / delta);
        infoLogger.info(NUM_RATE_OF + "elastic         " + numberOfPagesAddedToElastic + "\t" + (double) (numberOfPagesAddedToElastic - lastNumberOfPagesAddedToElastic) / delta);
        lastNumberOfUrlReceived = numberOfUrlReceived;
        lastNumberOfDuplicate = numberOfDuplicate;
        lastNumberOfDomainError = numberOfDomainError;
        lastNumberOfLanguagePassed = numberOfLanguagePassed;
        lastNumberOfCrawledPage = numberOFCrawledPage;
        lastNumberOfPagesAddedToHBase = numberOfPagesAddedToHBase;
        lastNumberOfPagesAddedToElastic = numberOfPagesAddedToElastic;
        lastTime = System.currentTimeMillis();
    }

    public static void stat(PrintStream out) {
        int delta = (int) ((System.currentTimeMillis() - lastTime) / 1000);
        out.println("received MB     " + (byteCounter >> B_TO_MB_BIT_SHIFT));
        out.println(NUM_RATE_OF + "received url    " + numberOfUrlReceived + "\t" + (double) (numberOfUrlReceived - lastNumberOfUrlReceived) / delta);
        out.println(NUM_RATE_OF + "passed lang     " + numberOfLanguagePassed + "\t" + (double) (numberOfLanguagePassed - lastNumberOfLanguagePassed) / delta);
        out.println(NUM_RATE_OF + "domain Error    " + numberOfDomainError + "\t" + (double) (numberOfDomainError - lastNumberOfDomainError) / delta);
        out.println(NUM_RATE_OF + "duplicate       " + numberOfDuplicate + "\t" + (double) (numberOfDuplicate - lastNumberOfDuplicate) / delta);
        out.println(NUM_RATE_OF + "crawl           " + numberOFCrawledPage + "\t" + (double) (numberOFCrawledPage - lastNumberOfCrawledPage) / delta);
        out.println(NUM_RATE_OF + "HBase           " + numberOfPagesAddedToHBase + "\t" + (double) (numberOfPagesAddedToHBase - lastNumberOfPagesAddedToHBase) / delta);
        out.println(NUM_RATE_OF + "elastic         " + numberOfPagesAddedToElastic + "\t" + (double) (numberOfPagesAddedToElastic - lastNumberOfPagesAddedToElastic) / delta);
        lastNumberOfUrlReceived = numberOfUrlReceived;
        lastNumberOfDuplicate = numberOfDuplicate;
        lastNumberOfDomainError = numberOfDomainError;
        lastNumberOfLanguagePassed = numberOfLanguagePassed;
        lastNumberOfCrawledPage = numberOFCrawledPage;
        lastNumberOfPagesAddedToHBase = numberOfPagesAddedToHBase;
        lastNumberOfPagesAddedToElastic = numberOfPagesAddedToElastic;
        lastTime = System.currentTimeMillis();
    }
}
