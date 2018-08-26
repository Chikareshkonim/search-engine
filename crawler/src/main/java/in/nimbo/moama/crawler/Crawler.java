package in.nimbo.moama.crawler;

import in.nimbo.moama.crawler.domainvalidation.UrlHandler;
import in.nimbo.moama.database.ElasticWebDaoImp;
import in.nimbo.moama.database.HBaseWebDaoImp;
import in.nimbo.moama.database.WebDao;
import in.nimbo.moama.database.webdocumet.WebDocument;
import in.nimbo.moama.exception.DomainFrequencyException;
import in.nimbo.moama.exception.DuplicateLinkException;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.exception.URLException;
import in.nimbo.moama.metrics.Metrics;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

import static java.lang.Thread.MAX_PRIORITY;
import static java.lang.Thread.sleep;

public class Crawler implements Runnable {
    private static final int NUMBER_OF_THREADS = 400;
    public static final int SLEEP_TIME = 35;
    private static Logger errorLogger = Logger.getLogger("error");
    private Parser parser;
    private URLQueue urlQueue;
    private URLQueue tempUrlQueue;
    private WebDao elasticDao;
    private WebDao hbaseDoa;

    public Crawler(URLQueue urlQueue, URLQueue tempUrlQueue) {
        this.urlQueue = urlQueue;
        this.tempUrlQueue = tempUrlQueue;
        parser = Parser.getInstance();
        System.out.println("end of crawler constructor");
        hbaseDoa = new HBaseWebDaoImp();
        hbaseDoa.createTable();
        elasticDao = new ElasticWebDaoImp();
    }

    @Override
    public void run() {
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            try {
                //To make sure CPU can handle sudden start of many threads
                sleep(SLEEP_TIME);
            } catch (InterruptedException ignored) {
            }
            Thread thread = new Thread(() -> {
                LinkedList<String> urlsOfThisThread = new LinkedList<>(urlQueue.getUrls());
                while (true) {
                    if (urlsOfThisThread.size() < 10) {
                        urlsOfThisThread.addAll(urlQueue.getUrls());
                    } else {
                        WebDocument webDocument;
                        String url = urlsOfThisThread.pop();
                        try {
                            webDocument = parser.parse(url);
                            Metrics.byteCounter += webDocument.getTextDoc().getBytes().length;
                            tempUrlQueue.pushNewURL(giveGoodLink(webDocument));
                            hbaseDoa.put(webDocument);
                            elasticDao.put(webDocument);
                        } catch (RuntimeException e) {
                            errorLogger.error("important" + e.getMessage());
                            throw e;
                        } catch (URLException | DuplicateLinkException | IOException | IllegalLanguageException | DomainFrequencyException ignored) {
                        }
                    }
                }
            });
            thread.setPriority(MAX_PRIORITY - 1);
            thread.start();
        }
    }

    private String[] giveGoodLink(WebDocument webDocument) throws MalformedURLException {
        ArrayList<String> externalLink = new ArrayList<>();
        ArrayList<String> internalLink = new ArrayList<>();
        UrlHandler.splitter(webDocument.getLinks(), internalLink, externalLink, new URL(webDocument.getPagelink()).getHost());
        if (internalLink.size() > 10) {
            Collections.shuffle(internalLink);
            externalLink.addAll(internalLink.subList(0, 10));
        } else {
            externalLink.addAll(internalLink);
        }
        return externalLink.toArray(new String[0]);
    }
}

