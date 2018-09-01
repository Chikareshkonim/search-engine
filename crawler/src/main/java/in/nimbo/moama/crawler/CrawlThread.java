package in.nimbo.moama.crawler;

import in.nimbo.moama.ElasticManager;
import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.WebDocumentHBaseManager;
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
import in.nimbo.moama.metrics.JMXManager;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.CrawlerPropertyType;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

public class CrawlThread extends Thread {
    private static final Logger errorLogger = Logger.getLogger(CrawlThread.class);
    private final boolean isRun;
    private static final Parser parser;
    private static final MoamaProducer helperProducer;
    private static final MoamaConsumer linkConsumer;
    private static final MoamaProducer crawledProducer;
    private static JMXManager jmxManager;
    private static final ElasticManager elasticManager;
    private static final WebDocumentHBaseManager webDocumentHBaseManager;
    private static int minOfEachQueue;
    private static int numOfInternalLinksToKafka;
    private static final DuplicateHandler DuplicateChecker = DuplicateHandler.getInstance();
    private static final DomainFrequencyHandler domainTimeHandler = DomainFrequencyHandler.getInstance();

    static {
        jmxManager = JMXManager.getInstance();
        crawledProducer = new MoamaProducer(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_CRAWLED_TOPIC_NAME)
                , "kafka.crawled.");
        helperProducer = new MoamaProducer(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_HELPER_TOPIC_NAME)
                , "kafka.helper.");
        linkConsumer = new MoamaConsumer(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_LINK_TOPIC_NAME)
                , "kafka.server.");
        minOfEachQueue = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_MIN_OF_EACH_THREAD_QUEUE));
        numOfInternalLinksToKafka = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA));
        String hBaseTable = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_TABLE);
        String scoreFamily = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_FAMILY_SCORE);
        String outLinksFamily = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_FAMILY_OUTLINKS);
        webDocumentHBaseManager = new WebDocumentHBaseManager(hBaseTable, outLinksFamily, scoreFamily);
        elasticManager = new ElasticManager();
        webDocumentHBaseManager.createTable();
        parser = Parser.getInstance();
    }

    public CrawlThread(boolean isRun) {
        this.isRun = isRun;

    }

    @Override
    public void run() {
        LinkedList<String> urlsOfThisThread = new LinkedList<>(linkConsumer.getDocuments());
        while (isRun) {
            work(urlsOfThisThread);
        }
        helperProducer.pushNewURL(urlsOfThisThread.toArray(new String[0]));
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
                crawledProducer.pushNewURL(url);
                webDocumentHBaseManager.put(webDocument.documentToJson(), jmxManager);
                elasticManager.put(webDocument.documentToJson(), jmxManager);
                Metrics.numberOFComplete++;// TODO: 8/31/18
                jmxManager.markNewComplete();
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
            } catch (MalformedURLException e) {
                errorLogger.error(url + " is malformed!");
            } catch (IOException e) {
                errorLogger.error("Jsoup connection to " + url + " failed");
            } catch (IllegalLanguageException e) {
                errorLogger.error("Couldn't recognize url language!" + url);
            } catch (DomainFrequencyException e) {
                errorLogger.error("take less than 30s to request to " + url);
            } catch (URLException | DuplicateLinkException e) {
                errorLogger.error(e.getMessage() + url);
            } catch (RuntimeException e) {
                errorLogger.error("important" + e.getMessage());
                throw e;
            }
        }
    }

    private void checkLink(String url) throws URLException, DomainFrequencyException, DuplicateLinkException, MalformedURLException {
        if (url == null) {
            jmxManager.markNewNull();
            throw new URLException();
        } else if (!domainTimeHandler.isAllow(new URL(url).getHost())) {
            Metrics.numberOfDomainError++;
            jmxManager.markNewDomainError();
            throw new DomainFrequencyException();
        }
        if (DuplicateChecker.isDuplicate(url)) {
            Metrics.numberOfDuplicate++;
            jmxManager.markNewDuplicate();
            throw new DuplicateLinkException();
        }

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
}