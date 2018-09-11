package in.nimbo.moama.crawler;

import in.nimbo.moama.Tuple;
import in.nimbo.moama.Utils;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.domainvalidation.DomainFrequencyHandler;
import in.nimbo.moama.crawler.domainvalidation.DuplicateHandler;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.metrics.FloatMeter;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.util.CrawlerPropertyType;
import org.jsoup.Jsoup;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;

public class PageFetcher {
    private static final IntMeter DUPLICATE_METER = new IntMeter("duplicate");
    private static final IntMeter NEW_URL_METER = new IntMeter("passed checking url");
    private static final IntMeter DOMAIN_ERROR_METER = new IntMeter("domain Error");
    private static final IntMeter NULL_URL_METER = new IntMeter("null url");
    private static final IntMeter FETCHED_URL = new IntMeter("fetched url");
    private static final IntMeter EXCEPTION_IN_FETCH = new IntMeter("exception in fetch");
    private static FloatMeter checkTime = new FloatMeter("check url Time ");
    private static FloatMeter fetchTime = new FloatMeter("fetch Page Time");
    private static final DuplicateHandler duplicateChecker = DuplicateHandler.getInstance();
    private static final DomainFrequencyHandler domainTimeHandler = DomainFrequencyHandler.getInstance();
    private static MoamaConsumer linkConsumer;
    private static LinkedBlockingQueue<String> checkedUrlsQueue = new LinkedBlockingQueue<>(400);
    private Boolean isFetching = true;
    private Boolean isConsuming = true;
    private LinkedBlockingQueue<ArrayList<Tuple<String, String>>> netFeteched;
    private static PageFetcher ourInstance = new PageFetcher();

    public static PageFetcher getInstance() {
        return ourInstance;
    }

    static {
        linkConsumer = new MoamaConsumer(ConfigManager.getInstance()
                .getProperty(CrawlerPropertyType.CRAWLER_LINK_TOPIC_NAME), "kafka.server.");
    }

    public ArrayList<Thread> fetchers = new ArrayList<>();
    public ConsumeState consumeState;

    private PageFetcher() {
    }

    public void run() {
        this.netFeteched = CrawlThread.netFeteched;
        for (int i = 0; i < 15; i++) {
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
                } catch (InterruptedException |ArrayIndexOutOfBoundsException|IllegalArgumentException ignored) {
                }
                });
            }
            });
            consumeThread.setPriority(8);
            consumeThread.start();
        }


        Utils.delay(5000);
        System.out.println("start fetching");
        for (int e = 0; e <300 ; e++) {
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
                        if (threadFetch.size() > 10) {
                            try {
                                fetchingState = FetchingState.put;
                                netFeteched.put(threadFetch);
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
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
            fetchers.add(thread);
            thread.start();
        }
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
        duplicateChecker.weakConfirm(url);
        return true;
    }

    enum ConsumeState {
        kafka, addBlocking
    }

    enum FetchingState {
        put, takeUrlQueue, fetchBody
    }

}
