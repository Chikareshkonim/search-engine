package in.nimbo.moama.metrics;

import org.apache.log4j.Logger;

import java.io.PrintStream;

import static java.lang.Thread.sleep;

public class Metrics {
    public static int numberOfUrlReceived = 0;
    public static int numberOfNull = 0;
    public static int numberOfDuplicate = 0;
    public static int numberOfDomainError = 0;
    public static long byteCounter = 0;
    public static int numberOFCrawledPage = 0;
    public static int numberOfLanguagePassed = 0;
    public static int numberOfPagesAddedToElastic = 0;
    public static int numberOfPagesAddedToHBase = 0;
    private static long lastTime = System.currentTimeMillis();
    private static Logger infoLogger = Logger.getLogger("info");
    private static int lastNumberOfLanguagePassed = 0;
    private static int lastNumberOfCrawledPage = 0;
    private static int lastNumberOfDuplicate = 0;
    private static int lastNumberOfDomainError = 0;
    private static int lastNumberOfUrlReceived = 0;
    private static int lastNumberOfPagesAddedToElastic = 0;
    private static int lastNumberOfPagesAddedToHBase = 0;
    public static void loadStat(){
    }

    public static void logStat() {
        int delta = (int) ((System.currentTimeMillis() - lastTime) / 1000);
        infoLogger.info("received MB     " + (byteCounter >> 20));
        infoLogger.info("num/rate of received url     " + numberOfUrlReceived + "\t" + (double) (numberOfUrlReceived - lastNumberOfUrlReceived) / delta);
        infoLogger.info("num/rate of passed lang    " + numberOfLanguagePassed + "\t" + (double) (numberOfLanguagePassed - lastNumberOfLanguagePassed) / delta);
        infoLogger.info("num/rate of duplicate      " + numberOfDuplicate + "\t" + (double) (numberOfDuplicate - lastNumberOfDuplicate) / delta);
        infoLogger.info("num/rate of domain Error   " + numberOfDomainError + "\t" + (double) (numberOfDomainError - lastNumberOfDomainError) / delta);
        infoLogger.info("num/rate of crawl         " + numberOFCrawledPage + "\t" + (double) (numberOFCrawledPage - lastNumberOfCrawledPage) / delta);
        infoLogger.info("num/rate of HBase         " + numberOfPagesAddedToHBase + "\t" + (double) (numberOfPagesAddedToHBase - lastNumberOfPagesAddedToHBase) / delta);
        infoLogger.info("num/rate of elastic         " + numberOfPagesAddedToElastic + "\t" + (double) (numberOfPagesAddedToElastic - lastNumberOfPagesAddedToElastic) / delta);
        infoLogger.info(numberOFCrawledPage + "number of crawled pages");
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
        out.println("received MB     " + (byteCounter >> 20));
        out.println("num/rate of received url     " + numberOfUrlReceived + "\t" + (double) (numberOfUrlReceived - lastNumberOfUrlReceived) / delta);
        out.println("num/rate of passed lang    " + numberOfLanguagePassed + "\t" + (double) (numberOfLanguagePassed - lastNumberOfLanguagePassed) / delta);
        out.println("num/rate of domain Error   " + numberOfDomainError + "\t" + (double) (numberOfDomainError - lastNumberOfDomainError) / delta);
        out.println("num/rate of duplicate      " + numberOfDuplicate + "\t" + (double) (numberOfDuplicate - lastNumberOfDuplicate) / delta);
        out.println("num/rate of crawl         " + numberOFCrawledPage + "\t" + (double) (numberOFCrawledPage - lastNumberOfCrawledPage) / delta);
        out.println("num/rate of HBase         " + numberOfPagesAddedToHBase + "\t" + (double) (numberOfPagesAddedToHBase - lastNumberOfPagesAddedToHBase) / delta);
        out.println("num/rate of elastic         " + numberOfPagesAddedToElastic + "\t" + (double) (numberOfPagesAddedToElastic - lastNumberOfPagesAddedToElastic) / delta);
        out.println(numberOFCrawledPage + "number of crawled pages");
        lastNumberOfUrlReceived = numberOfUrlReceived;
        lastNumberOfDuplicate = numberOfDuplicate;
        lastNumberOfDomainError = numberOfDomainError;
        lastNumberOfLanguagePassed = numberOfLanguagePassed;
        lastNumberOfCrawledPage = numberOFCrawledPage;
        lastNumberOfPagesAddedToHBase = numberOfPagesAddedToHBase;
        lastNumberOfPagesAddedToElastic = numberOfPagesAddedToElastic;
        lastTime = System.currentTimeMillis();


    }

    static {
        new Thread(() -> {
            while (true) {
                try {
                    sleep(20000);
                    logStat();
                } catch (InterruptedException ignored) {
                }
            }
        }).start();

    }
}
