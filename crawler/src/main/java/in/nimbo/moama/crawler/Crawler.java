package in.nimbo.moama.crawler;

import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.WebDocument;
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

import static in.nimbo.moama.util.Constants.*;
import static java.lang.Thread.sleep;

public class Crawler implements Runnable {
    private static Logger errorLogger = Logger.getLogger("error");
    private Parser parser;
    private URLQueue urlQueue;
    private URLQueue tempUrlQueue;
//    private WebDao elasticDao;
//    private WebDao hbaseDoa;

    public Crawler(URLQueue urlQueue, URLQueue tempUrlQueue) {
        this.urlQueue = urlQueue;
        this.tempUrlQueue = tempUrlQueue;
        parser = Parser.getInstance();
        System.out.println("end of crawler constructor");
//        hbaseDoa = new HBaseWebDaoImp();
//        hbaseDoa.createTable();
//        elasticDao = new ElasticWebDaoImp();
    }

    @Override
    public void run() {
        for (int i = 0; i < CRAWLER_NUMBER_OF_THREADS; i++) {
            try {
                //To make sure CPU can handle sudden start of many threads
                sleep(CRAWLER_START_NEW_THREAD_DELAY_MS);
            } catch (InterruptedException ignored) {
            }
            Thread thread = new Thread(() -> {
                LinkedList<String> urlsOfThisThread = new LinkedList<>(urlQueue.getUrls());
                while (true) {
                    if (urlsOfThisThread.size() < CRAWLER_MIN_OF_EACH_THREAD_QUEUE) {
                        urlsOfThisThread.addAll(urlQueue.getUrls());
                    } else {
                        WebDocument webDocument;
                        String url = urlsOfThisThread.pop();
                        try {
                            webDocument = parser.parse(url);
                            Metrics.byteCounter += webDocument.getTextDoc().getBytes().length;
                            tempUrlQueue.pushNewURL(giveGoodLink(webDocument));
//                            hbaseDoa.put(webDocument);
//                            elasticDao.put(webDocument);
                        } catch (RuntimeException e) {
                            errorLogger.error("important" + e.getMessage());
                            throw e;
                        } catch (URLException | DuplicateLinkException | IOException | IllegalLanguageException | DomainFrequencyException ignored) {
                        }
                    }
                }
            });
            thread.setPriority(CRAWLER_THREAD_PRIORITY);
            thread.start();
        }
    }

    private String[] giveGoodLink(WebDocument webDocument) throws MalformedURLException {
        ArrayList<String> externalLink = new ArrayList<>();
        ArrayList<String> internalLink = new ArrayList<>();
        UrlHandler.splitter(webDocument.getLinks(), internalLink, externalLink, new URL(webDocument.getPagelink()).getHost());
        if (internalLink.size() > CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA) {
        UrlHandler.splitter(webDocument.getLinks(), internalLink, externalLink, new URL(webDocument.getPageLink()).getHost());
        if (internalLink.size() > NUMBER_OF_OWN_LINK_READ) {
            Collections.shuffle(internalLink);
            externalLink.addAll(internalLink.subList(0, CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA));
        } else {
            externalLink.addAll(internalLink);
        }
        return externalLink.toArray(new String[0]);
    }
}

