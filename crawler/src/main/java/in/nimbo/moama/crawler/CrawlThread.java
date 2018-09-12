package in.nimbo.moama.crawler;

import in.nimbo.moama.*;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.document.Link;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.elasticsearch.ElasticManager;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.kafka.MoamaProducer;
import in.nimbo.moama.metrics.FloatMeter;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.util.CrawlerPropertyType;
import in.nimbo.moama.util.ElasticPropertyType;
import in.nimbo.moama.util.HBasePropertyType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static in.nimbo.moama.crawler.CrawlThread.CrawlState.*;


public class CrawlThread extends Thread {
    public static LinkedList<String> fatalErrors = new LinkedList<>();

    private static final IntMeter FATAL_ERROR = new IntMeter("FATAL error");
    private static final IntMeter COMPLETE_METER = new IntMeter("complete url");
    private static final FloatMeter MB_CRAWLED = new FloatMeter("MB Crawled");
    private static final FloatMeter PARSE_TIME = new FloatMeter("parse url Time ");
    private static final FloatMeter HBASE_PUT_TIME = new FloatMeter("hbase put Time ");
    private static final FloatMeter ELASTIC_PUT_TIME = new FloatMeter("elastic put Time");
    private static final FloatMeter DOCUMENT_CREATE_TIME = new FloatMeter("jsoup document create time");
    private static final Logger LOGGER = Logger.getLogger(CrawlThread.class);

    private boolean isRun = true;
    private static final Parser parser;
    private static final MoamaProducer helperProducer;
    private static final MoamaProducer crawledProducer;
    private static final ElasticManager elasticManager;
    private static final WebDocumentHBaseManager webDocumentHBaseManager;
    private static int numOfInternalLinksToKafka;
    private static String hBaseTable;
    private static String scoreFamily;
    private static String outLinksFamily;
    private static int hbaseSizeLimit;
    private static int elasticSizeBulkLimit;
    private ArrayList<Tuple<String, String>> batchDocsOfThisThread;
    private LinkedList<Put> webDocOfThisThread;
    private List<Map<String, String>> elasticDocOfThisThread;
    private CrawlState threadCrawlState = null;

    static {
        elasticSizeBulkLimit = ConfigManager.getInstance().getIntProperty(ElasticPropertyType.ELASTIC_FLUSH_SIZE_LIMIT);
        hbaseSizeLimit = ConfigManager.getInstance().getIntProperty(HBasePropertyType.PUT_SIZE_LIMIT);
        numOfInternalLinksToKafka = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA);
        outLinksFamily = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_FAMILY_OUTLINKS);
        scoreFamily = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_FAMILY_SCORE);
        hBaseTable = ConfigManager.getInstance().getProperty(CrawlerPropertyType.HBASE_TABLE);
        crawledProducer = new MoamaProducer(ConfigManager.getInstance()
                .getProperty(CrawlerPropertyType.CRAWLER_CRAWLED_TOPIC_NAME), "kafka.crawled.");
        helperProducer = new MoamaProducer(ConfigManager.getInstance()
                .getProperty(CrawlerPropertyType.CRAWLER_HELPER_TOPIC_NAME), "kafka.helper.");
        webDocumentHBaseManager = new WebDocumentHBaseManager(hBaseTable, outLinksFamily, scoreFamily);
        elasticManager = new ElasticManager();
        webDocumentHBaseManager.createTable();
        parser = Parser.getInstance();
    }

    @Override
    public void run() {
        batchDocsOfThisThread = new ArrayList<>();
        webDocOfThisThread = new LinkedList<>();
        elasticDocOfThisThread = new LinkedList<>();
        while (isRun) {
            work();
        }
        end();
    }

    private long tempTime;
    static LinkedBlockingQueue<ArrayList<Tuple<String, String>>> netFetched = new LinkedBlockingQueue<>();

    private void work() {
        try {
            batchDocsOfThisThread = netFetched.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (Tuple<String, String> doc : batchDocsOfThisThread) {
            WebDocument webDocument;
            String url = doc.getX();
            String body = doc.getY();
            try {
                Document document = getDocument(body);
                MB_CRAWLED.add((double) document.outerHtml().getBytes().length / 0b100000000000000000000);
                webDocument = createWebDocument(url, document);
                helperProducer.pushNewURL(normalizeOutLink(webDocument));
                crawledProducer.pushNewURL(url);
                dataBasePut(webDocument);
                COMPLETE_METER.increment();// TODO: 8/31/18
            } catch (IllegalLanguageException ignored) {
            } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
                //this two exception ignored for our bug (and some in jsoup)
                LOGGER.warn(e.getMessage(), e);
            } catch (MalformedURLException e) {
                LOGGER.warn(url + " is malformed!");
            } catch (RuntimeException e) {
                FATAL_ERROR.increment();
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                fatalErrors.add(sw.toString());
                LOGGER.error("important" + e.getMessage(), e);
                throw e;
            }
        }
    }


    private void dataBasePut(WebDocument webDocument) {
        threadCrawlState = hbase;
        tempTime = System.currentTimeMillis();
        webDocOfThisThread.add(createHbasePut(webDocument.getPageLink(), webDocument.getLinks()));
        if (webDocOfThisThread.size() > hbaseSizeLimit) {
            webDocumentHBaseManager.puts(webDocOfThisThread);
            webDocOfThisThread = new LinkedList<>();
        }
        HBASE_PUT_TIME.add((double) (System.currentTimeMillis() - tempTime) / 1000);
        //////
        threadCrawlState = elastic;
        tempTime = System.currentTimeMillis();
        elasticDocOfThisThread.add(webDocument.elasticMap());
        if (elasticDocOfThisThread.size() > elasticSizeBulkLimit) {
            elasticManager.put(elasticDocOfThisThread);
            elasticDocOfThisThread.clear();
        }
        ELASTIC_PUT_TIME.add((float) (System.currentTimeMillis() - tempTime) / 1000);
    }

    private WebDocument createWebDocument(String url, Document document) throws IllegalLanguageException, MalformedURLException {
        WebDocument webDocument;
        threadCrawlState = parse;
        tempTime = System.currentTimeMillis();
        webDocument = parser.parse(document, url);
        PARSE_TIME.add((double) (System.currentTimeMillis() - tempTime) / 1000);
        return webDocument;
    }

    private Document getDocument(String body) {
        threadCrawlState = document;
        tempTime = System.currentTimeMillis();
        Document document = Jsoup.parse(body);
        DOCUMENT_CREATE_TIME.add((double) (System.currentTimeMillis() - tempTime) / 1000);
        return document;
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


    private String[] normalizeOutLink(WebDocument webDocument) throws MalformedURLException {
        List<Link> externalLink = new ArrayList<>();
        List<Link> internalLink = new ArrayList<>();
        UrlHandler.splitter(webDocument.getLinks(), internalLink, externalLink, new URL(webDocument.getPageLink()).getHost());
        if (internalLink.size() > numOfInternalLinksToKafka) {
            Collections.shuffle(internalLink);
            externalLink.addAll(internalLink.subList(0, numOfInternalLinksToKafka));
        } else {
            externalLink.addAll(internalLink);
        }
        webDocument.setLinks(externalLink);
        return externalLink.stream().map(Link::getUrl).toArray(String[]::new);
    }

    public void off() {
        isRun = false;
    }

    public void end() {
        webDocumentHBaseManager.put(webDocOfThisThread);
        elasticManager.put(elasticDocOfThisThread);
        helperProducer.pushNewURL(batchDocsOfThisThread.stream()
                .map(Tuple::getX).toArray(String[]::new));
    }

    public static void exiting() {
        elasticManager.close();
        webDocumentHBaseManager.close();
    }

    public CrawlState getThreadCrawlState() {
        return threadCrawlState;
    }


    public enum CrawlState {
        document, hbase, parse, elastic

    }
}