package in.nimbo.moama.crawler;

import in.nimbo.moama.Tuple;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.domainvalidation.DomainFrequencyHandler;
import in.nimbo.moama.crawler.domainvalidation.DuplicateHandler;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.metrics.FloatMeter;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.elasticsearch.util.CrawlerPropertyType;
import org.jsoup.Jsoup;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

public class PageFetcher {
    private static final IntMeter DUPLICATE_METER = new IntMeter("passed checking url");
    private static final IntMeter NEW_URL_METER = new IntMeter("new url");
    private static final IntMeter DOMAIN_ERROR_METER = new IntMeter("domain Error");
    private static final IntMeter NULL_URL_METER = new IntMeter("null url");
    private static FloatMeter checkTime = new FloatMeter("check url Time ");
    private static FloatMeter fetchTime = new FloatMeter("fetch Page Time");
    private static final DuplicateHandler duplicateChecker = DuplicateHandler.getInstance();
    private static final DomainFrequencyHandler domainTimeHandler = DomainFrequencyHandler.getInstance();
    private static MoamaConsumer linkConsumer;
    private Boolean isFetching = true;
    private Boolean isConsuming = true;
    private ArrayBlockingQueue<String> checkedUrlsQueue;
    private ArrayBlockingQueue<Tuple<String, String>> netFeteched;

    static {
        linkConsumer = new MoamaConsumer(ConfigManager.getInstance()
                .getProperty(CrawlerPropertyType.CRAWLER_LINK_TOPIC_NAME), "kafka.server.");
    }

    public PageFetcher() {
        this.checkedUrlsQueue = CrawlThread.checkedUrlsQueue;
        this.netFeteched = CrawlThread.netFeteched;

        Thread consumeThread=new Thread(() -> {
            while (isConsuming) {
                System.out.println("  checked size "+checkedUrlsQueue.size());
                System.out.println("  fetch queueSize  "+ netFeteched.size());
                linkConsumer.getDocuments().stream().filter(PageFetcher::checkLink)
                        .forEach(url -> {
                            try {
                                checkedUrlsQueue.put(url);
                            } catch (InterruptedException ignored) {
                            }
                        });
            }
        });
        consumeThread.start();

        ForkJoinPool forkJoinPool = new ForkJoinPool(4);
        IntStream.range(0,300).forEach(e->new Thread(() -> {
            while (isFetching) {
                String url;
                try {
                    url = checkedUrlsQueue.take();
                    String body = Jsoup.connect(url).validateTLSCertificates(false).ignoreHttpErrors(true).timeout(1500)
                            .execute().body();
                    System.out.println("execute url");
                    netFeteched.put(new Tuple<>(url, body));
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }
        }).start());
    }

    private static boolean checkLink(String url) {
        if (url == null) {
            NULL_URL_METER.increment();
            return false;
        } else {
            try {
                if (!domainTimeHandler.isAllow(new URL(url).getHost())) {
                    DOMAIN_ERROR_METER.increment();
                    return false;
                }
            } catch (MalformedURLException e) {
                return false;
            }
        }
        if (duplicateChecker.isDuplicate(url)) {
            DUPLICATE_METER.increment();
            return false;
        }
        NEW_URL_METER.increment();
        return true;
    }

}
