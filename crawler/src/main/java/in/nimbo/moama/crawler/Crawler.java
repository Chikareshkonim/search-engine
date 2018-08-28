package in.nimbo.moama.crawler;

import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.exception.DomainFrequencyException;
import in.nimbo.moama.exception.DuplicateLinkException;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.exception.URLException;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.kafka.MoamaProducer;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.PropertyType;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;
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
    private ConfigManager configManager;
    private static int minOfEachQueue;
    private static int threadPriority;
    private static  int shuffleSize;
    private static int numOfInternalLinksToKafka;
    private static int numOfThreads;
    private static int startNewThreadDelay;
    public Crawler() {
        String configAddress= new File(getClass().getClassLoader().getResource("config.properties").getFile()).getAbsolutePath();
        try {
            configManager = new ConfigManager(configAddress, PROPERTIES);
        } catch (IOException e) {
            errorLogger.error("Loading properties failed");
        }
        //TODO
        mainProducer = new MoamaProducer("links", configAddress);
        helperProducer = new MoamaProducer("helper", configAddress);
        documentProducer = new MoamaProducer("documents", configAddress);
        linkConsumer = new MoamaConsumer("links", configAddress);
        helperConsumer = new MoamaConsumer("helper", configAddress);
        minOfEachQueue = Integer.parseInt(configManager.getProperty(PropertyType.CRAWLER_MIN_OF_EACH_THREAD_QUEUE));
        threadPriority = Integer.parseInt(configManager.getProperty(PropertyType.CRAWLER_THREAD_PRIORITY));
        shuffleSize = Integer.parseInt(configManager.getProperty(PropertyType.CRAWLER_SHUFFLE_SIZE));
        numOfInternalLinksToKafka = Integer.parseInt(configManager.getProperty(PropertyType.CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA));
        numOfThreads= Integer.parseInt(configManager.getProperty(PropertyType.CRAWLER_NUMBER_OF_THREADS));
        startNewThreadDelay= Integer.parseInt(configManager.getProperty(PropertyType.CRAWLER_START_NEW_THREAD_DELAY_MS));
        parser = Parser.getInstance(configManager);
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
            try {
                //To make sure CPU can handle sudden start of many threads
                sleep(startNewThreadDelay);
            } catch (InterruptedException ignored) {
            }
            Thread thread = new Thread(() -> {
                LinkedList<String> urlsOfThisThread = new LinkedList<>(linkConsumer.getDocuments());
                while (true) {
                    if (urlsOfThisThread.size() < minOfEachQueue) {
                        urlsOfThisThread.addAll(linkConsumer.getDocuments());
                    } else {
                        WebDocument webDocument;
                        String url = urlsOfThisThread.pop();
                        try {
                            webDocument = parser.parse(url);
                            Metrics.byteCounter += webDocument.getTextDoc().getBytes().length;
                            helperProducer.pushNewURL(giveGoodLink(webDocument));
                            //TODO
                            documentProducer.pushDocument(webDocument.documentToJson());
                        } catch (RuntimeException e) {
                            errorLogger.error("important" + e.getMessage());
                            throw e;
                        } catch (URLException | DuplicateLinkException | IOException | IllegalLanguageException | DomainFrequencyException ignored) {
                        }
                    }
                }
            });
            thread.setPriority(threadPriority);
            thread.start();
        }
    }

    private String[] giveGoodLink(WebDocument webDocument) throws MalformedURLException {
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

