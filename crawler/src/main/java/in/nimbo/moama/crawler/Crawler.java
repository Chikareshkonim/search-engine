package in.nimbo.moama.crawler;

import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.exception.DomainFrequencyException;
import in.nimbo.moama.exception.DuplicateLinkException;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.exception.URLException;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.kafka.MoamaProducer;
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
    private MoamaProducer mainProducer;
    private MoamaProducer helperProducer;
    private MoamaProducer documentProducer;
    private MoamaConsumer linkConsumer;
    private MoamaConsumer helperConsumer;
    private static final int SHUFFLE_SIZE = 200000;
    private static final int THREAD_INTERRUPTION=0;
    public Crawler() {
        //TODO
        mainProducer = new MoamaProducer("", "");
        helperProducer = new MoamaProducer("", "");
        documentProducer = new MoamaProducer("", "");
        linkConsumer = new MoamaConsumer("", "");
        helperConsumer = new MoamaConsumer("", "");
        parser = Parser.getInstance();
        try {
            manageKafkaHelper();
        } catch (InterruptedException e) {
            //FIXME
            errorLogger.error("link shuffling thread has been interrupted");
            System.exit(THREAD_INTERRUPTION);
        }
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
                LinkedList<String> urlsOfThisThread = new LinkedList<>(linkConsumer.getDocuments());
                while (true) {
                    if (urlsOfThisThread.size() < CRAWLER_MIN_OF_EACH_THREAD_QUEUE) {
                        urlsOfThisThread.addAll(linkConsumer.getDocuments());
                    } else {
                        WebDocument webDocument;
                        String url = urlsOfThisThread.pop();
                        try {
                            webDocument = parser.parse(url);
                            Metrics.byteCounter += webDocument.getTextDoc().getBytes().length;
                            helperProducer.pushNewURL(giveGoodLink(webDocument));
                            //TODO
                            documentProducer.pushDocument(webDocument.toString());
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
        UrlHandler.splitter(webDocument.getLinks(), internalLink, externalLink, new URL(webDocument.getPageLink()).getHost());
        if (internalLink.size() > CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA) {
            Collections.shuffle(internalLink);
            externalLink.addAll(internalLink.subList(0, CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA));
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
            if (linkedList.size() > SHUFFLE_SIZE) {
                Collections.shuffle(linkedList);
                mainProducer.pushNewURL(linkedList.toArray(new String[0]));
                linkedList.clear();
            }
        }
    }
}

