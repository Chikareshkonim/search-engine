package in.nimbo.moama.crawler;

import in.nimbo.moama.ElasticManager;
import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.WebDocumentHBaseManager;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.domainvalidation.DomainFrequencyHandler;
import in.nimbo.moama.crawler.domainvalidation.DuplicateHandler;
import in.nimbo.moama.document.Link;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.exception.DomainFrequencyException;
import in.nimbo.moama.exception.DuplicateLinkException;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.exception.URLException;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.kafka.MoamaProducer;
import in.nimbo.moama.metrics.FloatMeter;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.metrics.JMXManager;
import in.nimbo.moama.util.CrawlerPropertyType;
import in.nimbo.moama.util.ElasticPropertyType;
import in.nimbo.moama.util.HBasePropertyType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.UncheckedIOException;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class CrawlThread extends Thread {
    private static JMXManager jmxManager;
    private static final Logger LOGGER = Logger.getLogger(CrawlThread.class);
    private final boolean isRun;
    private static final Parser parser;
    private static final MoamaProducer helperProducer;
    private static final MoamaConsumer linkConsumer;
    private static final MoamaProducer crawledProducer;
    private static final ElasticManager elasticManager;
    private static final WebDocumentHBaseManager webDocumentHBaseManager;
    private static int minOfEachQueue;
    private static int numOfInternalLinksToKafka;
    private static final DuplicateHandler duplicateChecker = DuplicateHandler.getInstance();
    private static final DomainFrequencyHandler domainTimeHandler = DomainFrequencyHandler.getInstance();
    private static FloatMeter megaByteCounter = new FloatMeter("MB Crawled     ");
    private static IntMeter duplicate = new IntMeter("duplicate url    ");
    private static IntMeter complete = new IntMeter("complete      ");
    private static IntMeter domainError = new IntMeter("domain Error   ");
    private static IntMeter ioUncheckException = new IntMeter("Unchecked io Exception");
    private static IntMeter urlReceived=new IntMeter("url received");
    private static FloatMeter checkTime = new FloatMeter("check url Time ");
    private static FloatMeter parseTime = new FloatMeter("parse url Time ");
    private static FloatMeter hbaseTime = new FloatMeter("hbase put Time ");
    private static FloatMeter elasticTime = new FloatMeter("elastic put Time");
    private static FloatMeter fetchTime = new FloatMeter("fetch Page Time");
    private static FloatMeter documentTime = new FloatMeter("jsoup document create time");
    private static FloatMeter kafkaTime = new FloatMeter("kafka input time");
    private static String hBaseTable;
    private static String scoreFamily;
    private static String outLinksFamily;
    private static int hbaseSizeLimit;
    private static int elasticSizeBulkLimit;

    static {
        elasticSizeBulkLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_FLUSH_SIZE_LIMIT));
        hbaseSizeLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(HBasePropertyType.PUT_SIZE_LIMIT));
        outLinksFamily = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_FAMILY_OUTLINKS);
        scoreFamily = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_FAMILY_SCORE);
        hBaseTable = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_TABLE);
        jmxManager = JMXManager.getInstance();
        crawledProducer = new MoamaProducer(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_CRAWLED_TOPIC_NAME)
                , "kafka.crawled.");
        helperProducer = new MoamaProducer(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_HELPER_TOPIC_NAME)
                , "kafka.helper.");
        linkConsumer = new MoamaConsumer(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_LINK_TOPIC_NAME)
                , "kafka.server.");
        minOfEachQueue = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_MIN_OF_EACH_THREAD_QUEUE));
        numOfInternalLinksToKafka = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA));
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
        LinkedList<Put> webDocOfThisThread = new LinkedList<>();
        LinkedList<Map<String, String>> elasticDocOfThisThread = new LinkedList<>();

        while (isRun) {
            work(urlsOfThisThread, webDocOfThisThread, elasticDocOfThisThread);
        }
        helperProducer.pushNewURL(urlsOfThisThread.toArray(new String[0]));


    }

    private void work(LinkedList<String> urlsOfThisThread, LinkedList<Put> webDocOfThisThread, LinkedList<Map<String, String>> elasticDocOfThisThread) {
        if (urlsOfThisThread.size() < minOfEachQueue) {
            urlsOfThisThread.addAll(linkConsumer.getDocuments());
        } else {
            WebDocument webDocument;
            String url = urlsOfThisThread.pop();
            try {
                long tempTime = System.currentTimeMillis();
                checkLink(url);
                checkTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
                //////
                tempTime = System.currentTimeMillis();
                String string = Jsoup.connect(url).validateTLSCertificates(false).execute().body();
                fetchTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
                //////
                tempTime = System.currentTimeMillis();
                Document document = Jsoup.parse(string);
                documentTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
                //////

                duplicateChecker.weakConfirm(url);
                urlReceived.increment();
                tempTime = System.currentTimeMillis();
                webDocument = parser.parse(document, url);
                parseTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
                //////
                tempTime = System.currentTimeMillis();
                megaByteCounter.add((float) document.outerHtml().getBytes().length / 0b100000000000000000000);
                helperProducer.pushNewURL(normalizeOutLink(webDocument));
                crawledProducer.pushNewURL(url);
                kafkaTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
                //////
                // TODO: 9/2/18 mustChange
                /////
                tempTime = System.currentTimeMillis();
                webDocOfThisThread.add(createHbasePut(webDocument.getPageLink(), webDocument.getLinks()));
                if (webDocOfThisThread.size() > hbaseSizeLimit) {
                    webDocumentHBaseManager.put(webDocOfThisThread);
                }
                hbaseTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
                //////
                tempTime = System.currentTimeMillis();
                elasticDocOfThisThread.add(webDocument.elasticMap());
                if (elasticDocOfThisThread.size() > elasticSizeBulkLimit){
                    elasticManager.myput(elasticDocOfThisThread);
                    elasticDocOfThisThread.clear();
                }
                elasticTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
                //////
                complete.increment();// TODO: 8/31/18
                jmxManager.markNewComplete();

            } catch (DomainFrequencyException | DuplicateLinkException | IllegalLanguageException ignored) {
            } catch (UncheckedIOException e) {
                duplicateChecker.weakConfirm(url);
                ioUncheckException.increment();
                LOGGER.info(e.getMessage(), e);
            } catch (IllegalArgumentException e) {
                LOGGER.trace("IllegalArgumentException");
            } catch (MalformedURLException e) {
                duplicateChecker.weakConfirm(url);
                LOGGER.warn(url + " is malformed!");
            } catch (IOException e) {
                LOGGER.trace("Jsoup connection to " + url + " failed");
            } catch (URLException e) {
                duplicateChecker.weakConfirm(url);
                LOGGER.trace("url exception", e);
            } catch (RuntimeException e) {
                LOGGER.error("important" + e.getMessage(), e);
                throw e;
            }
        }
    }

    private Put createHbasePut(String pageLink, List<Link> outLink) {
        String pageRankColumn = ConfigManager.getInstance().getProperty(HBasePropertyType.HBASE_DUPCHECK_COLUMN);
        Put put = new Put(Bytes.toBytes(webDocumentHBaseManager.generateRowKeyFromUrl(pageLink)));
        for (Link link : outLink) {
            put.addColumn(outLinksFamily.getBytes(), webDocumentHBaseManager.generateRowKeyFromUrl(
                    link.getUrl()).getBytes(), (link.getAnchorLink()).getBytes());
        }
        put.addColumn(scoreFamily.getBytes(), pageRankColumn.getBytes(), Bytes.toBytes(1.0));
        return put;
    }

    private void checkLink(String url) throws URLException, DomainFrequencyException, DuplicateLinkException, MalformedURLException {
        if (url == null) {
            jmxManager.markNewNull();
            throw new URLException();
        } else if (!domainTimeHandler.isAllow(new URL(url).getHost())) {
            domainError.increment();
            jmxManager.markNewDomainError();
            throw new DomainFrequencyException();
        }
        if (duplicateChecker.isDuplicate(url)) {
            duplicate.increment();
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