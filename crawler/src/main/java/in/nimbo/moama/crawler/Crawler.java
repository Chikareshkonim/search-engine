package in.nimbo.moama.crawler;

import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.domainvalidation.DomainFrequencyHandler;
import in.nimbo.moama.crawler.domainvalidation.DuplicateHandler;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.exception.DomainFrequencyException;
import in.nimbo.moama.exception.DuplicateLinkException;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.exception.URLException;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.kafka.MoamaProducer;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.CrawlerPropertyType;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;
import static java.lang.Thread.sleep;

public class Crawler implements Runnable {
    private static Logger errorLogger = Logger.getLogger("error");
    private Parser parser;
    private MoamaProducer mainProducer;
    private MoamaProducer helperProducer;
    private MoamaProducer documentProducer;
    private MoamaConsumer linkConsumer;
    private MoamaConsumer helperConsumer;
    private ConfigManager configManager;
    private static int minOfEachQueue;
    private static int threadPriority;
    private static int shuffleSize;
    private static int numOfInternalLinksToKafka;
    private static int numOfThreads;
    private static int startNewThreadDelay;
    private DomainFrequencyHandler domainTimeHandler = DomainFrequencyHandler.getInstance();
    private DuplicateHandler DuplicateChecker = DuplicateHandler.getInstance();

    public Crawler() {
        InputStream fileInputStream = Crawler.class.getResourceAsStream("/crawler.properties");
        try {
            ConfigManager.getInstance().load(fileInputStream, PROPERTIES);
        } catch (IOException e) {
            errorLogger.error("Loading properties failed");
        }
        //TODO
        mainProducer = new MoamaProducer("links");
        helperProducer = new MoamaProducer("helper");
        documentProducer = new MoamaProducer("documents");
        linkConsumer = new MoamaConsumer("links");
        helperConsumer = new MoamaConsumer("helper");
        minOfEachQueue = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_MIN_OF_EACH_THREAD_QUEUE));
        threadPriority = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_THREAD_PRIORITY));
        shuffleSize = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_SHUFFLE_SIZE));
        numOfInternalLinksToKafka = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA));
        numOfThreads = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_NUMBER_OF_THREADS));
        startNewThreadDelay = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_START_NEW_THREAD_DELAY_MS));
        parser = Parser.getInstance(ConfigManager.getInstance());
        try {
            manageKafkaHelper();
        } catch (InterruptedException e) {
            //FIXME
            errorLogger.error("link shuffling thread has been interrupted");
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < numOfThreads; i++) {
            createCrawlingThread(true);
            try {
                //To make sure CPU can handle sudden start of many threads
                sleep(startNewThreadDelay);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private Thread createCrawlingThread(Boolean isRun) {
        Thread thread = new Thread(() -> {
            LinkedList<String> urlsOfThisThread = new LinkedList<>(linkConsumer.getDocuments());
            while (isRun) {
                work(urlsOfThisThread);
            }
            helperProducer.pushNewURL(urlsOfThisThread.toArray(new String[0]));

        });
        thread.setPriority(threadPriority);
        return thread;
    }

    private void work(LinkedList<String> urlsOfThisThread) {
        if (urlsOfThisThread.size() < minOfEachQueue) {
            urlsOfThisThread.addAll(linkConsumer.getDocuments());
        } else {
            WebDocument webDocument;
            String url = urlsOfThisThread.pop();
            try {
                checkLink(url);
                webDocument = parser.parse(url);
                Metrics.byteCounter += webDocument.getTextDoc().getBytes().length;
                helperProducer.pushNewURL(normalizeOutLink(webDocument));
                Metrics.numberOFComplete++;//todo
            } catch (RuntimeException e) {
                errorLogger.error("important" + e.getMessage());
                throw e;
            } catch (MalformedURLException e) {
                errorLogger.error(url + " is malformatted!");
            } catch (IOException e) {
                errorLogger.error("Jsoup connection to " + url + " failed");
            } catch (IllegalLanguageException e) {
                errorLogger.error("Couldn't recognize url language!" + url);
            } catch (DomainFrequencyException e) {
                errorLogger.error("take less than 30s to request to " + url);
            } catch (URLException e) {
                errorLogger.error("number of null" + Metrics.numberOfNull++);
            } catch (DuplicateLinkException e) {
                errorLogger.error(url + " is duplicate");
            }
        }
    }

    private void checkLink(String url) throws URLException, DomainFrequencyException, DuplicateLinkException {
        if (url == null) {
            throw new URLException();
        } else if (!domainTimeHandler.isAllow(url)) {
            Metrics.numberOfDomainError++;
            throw new DomainFrequencyException();
        }
        if (DuplicateChecker.isDuplicate(url)) {
            Metrics.numberOfDuplicate++;
            throw new DuplicateLinkException();
        }

    }

    private void work(Boolean isRun) {
        LinkedList<String> urlsOfThisThread = new LinkedList<>(linkConsumer.getDocuments());
        while (isRun) {
            work(urlsOfThisThread);
        }
        helperProducer.pushNewURL(urlsOfThisThread.toArray(new String[0]));
    }

    private String[] normalizeOutLink(WebDocument webDocument) throws MalformedURLException {
        ArrayList<String> externalLink = new ArrayList<>();
        ArrayList<String> internalLink = new ArrayList<>();
        UrlHandler.splitter(webDocument.getLinks(), internalLink, externalLink, new URL(webDocument.getPageLink()).getHost());
        if (internalLink.size() > numOfInternalLinksToKafka) {
            Collections.shuffle(internalLink);
            externalLink.addAll(internalLink.subList(0, numOfInternalLinksToKafka));
        } else {
            externalLink.addAll(internalLink);
        }
        return externalLink.toArray(new String[0]);
    }

    private void manageKafkaHelper() throws InterruptedException {
        LinkedList<String> linkedList = new LinkedList<>();
        while (true) {
            sleep(2000);
            linkedList.addAll(helperConsumer.getDocuments());
            if (linkedList.size() > shuffleSize) {
                Collections.shuffle(linkedList);
                mainProducer.pushNewURL(linkedList.toArray(new String[0]));
                linkedList.clear();
            }
        }
    }
}

