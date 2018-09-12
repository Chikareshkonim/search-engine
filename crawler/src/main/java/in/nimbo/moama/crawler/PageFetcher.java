package in.nimbo.moama.crawler;

import in.nimbo.moama.Tuple;
import in.nimbo.moama.Utils;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.domainvalidation.DomainFrequencyHandler;
import in.nimbo.moama.crawler.domainvalidation.DuplicateHandler;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.util.CrawlerPropertyType;
import org.jsoup.Jsoup;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class PageFetcher {
    private static final IntMeter DUPLICATE_METER = new IntMeter("duplicate");
    private static final IntMeter NEW_URL_METER = new IntMeter("new url");
    private static final IntMeter DOMAIN_ERROR_METER = new IntMeter("domain Error");
    private static final IntMeter NULL_URL_METER = new IntMeter("null url");
    private static final IntMeter FETCHED_URL = new IntMeter("url received");
    private static final IntMeter EXCEPTION_IN_FETCH = new IntMeter("exception in fetch");
    private static final DuplicateHandler duplicateChecker = DuplicateHandler.getInstance();
    private static final DomainFrequencyHandler domainTimeHandler = DomainFrequencyHandler.getInstance();
    private static MoamaConsumer linkConsumer;
    private static LinkedBlockingQueue<String> checkedUrlsQueue = new LinkedBlockingQueue<>(400);
    private static int fetcherBatchSize;
    private Boolean isFetching = true;
    private Boolean isConsuming = true;
    private LinkedBlockingQueue<ArrayList<Tuple<String, String>>> netFetched;
    private static PageFetcher ourInstance = new PageFetcher();
    private static int fetcherThreadsNumber;
    private static int consumerThreadsNumber;
    private static int consumerThreadsPriority;

    public static PageFetcher getInstance() {
        return ourInstance;
    }

    static {
        fetcherThreadsNumber = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_FETCHER_THREAD);
        consumerThreadsPriority = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_FETCHER_PRIORITY);
        consumerThreadsNumber = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_CONSUMER_THREADS);
        consumerThreadsPriority = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_CONSUMER_PRIORITY);
        fetcherBatchSize = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_FETCHER_BATCH_SIZE);
        linkConsumer = new MoamaConsumer(ConfigManager.getInstance()
                .getProperty(CrawlerPropertyType.CRAWLER_LINK_TOPIC_NAME), "kafka.server.");
    }

    public ArrayList<Thread> fetchers = new ArrayList<>();
    public ConsumeState consumeState;

    private PageFetcher() {
    }

    public void run() {
        this.netFetched = CrawlThread.netFetched;
        for (int i = 0; i < consumerThreadsNumber; i++) {
            Thread consumeThread = new Thread(() -> {
                while (isConsuming) {
                    consumeState = ConsumeState.kafka;
                    ArrayList<String> documents = linkConsumer.getDocuments();
                    consumeState = ConsumeState.addBlocking;
                    documents.forEach(url -> {
                        try {
                            if (checkLink(url)) {
                                checkedUrlsQueue.put(url);
                            }
                        } catch (InterruptedException | ArrayIndexOutOfBoundsException |ClassCastException | IllegalArgumentException ignored) {
                        }
                    });
                }
            });
            consumeThread.setPriority(consumerThreadsPriority);
            consumeThread.start();
        }
        Utils.delay(5000);
        for (int e = 0; e < fetcherThreadsNumber; e++) {
            Thread thread;
            thread = new Thread(new Runnable() {
                ArrayList<Tuple<String, String>> threadFetch = new ArrayList<>();

                @Override
                public String toString() {
                    return String.valueOf(fetchingState);
                }

                public FetchingState fetchingState = null;

                @Override
                public void run() {
                    while (isFetching) {
                        if (threadFetch.size() > fetcherBatchSize) {
                            try {
                                fetchingState = FetchingState.put;
                                netFetched.put(threadFetch);
                            } catch (InterruptedException ignored) {
                            }
                            threadFetch = new ArrayList<>();
                        }
                        String url;
                        try {
                            fetchingState = FetchingState.takeUrlQueue;
                            url = checkedUrlsQueue.take();
                            fetchingState = FetchingState.fetchBody;
                            String body = Jsoup.connect(url).validateTLSCertificates(false).ignoreHttpErrors(true).timeout(2000)
                                    .execute().body();
                            FETCHED_URL.increment();
                            threadFetch.add(new Tuple<>(url, body));
                        } catch (Exception ignored) {
                            EXCEPTION_IN_FETCH.increment();
                        }
                    }
                }
            });
            thread.setPriority(consumerThreadsPriority);
            fetchers.add(thread);
            thread.start();
        }
    }

    private static boolean checkLink(String url) {
        String host;
        if (url == null) {
            NULL_URL_METER.increment();
            return false;
        } else {
            try {
                host = new URL(url).getHost();
                if (!domainTimeHandler.isAllow(host)) {
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
        duplicateChecker.weakConfirm(url);
        domainTimeHandler.confirm(host);
        return true;
    }

    enum ConsumeState {
        kafka, addBlocking
    }

    enum FetchingState {
        put, takeUrlQueue, fetchBody
    }

}
