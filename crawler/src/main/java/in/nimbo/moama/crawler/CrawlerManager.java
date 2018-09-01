package in.nimbo.moama.crawler;

import in.nimbo.moama.configmanager.*;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.kafka.MoamaProducer;
import in.nimbo.moama.util.CrawlerPropertyType;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.LinkedList;

import static java.lang.Thread.sleep;

public class CrawlerManager implements Reconfigurable  {
    private static final Logger errorLogger = Logger.getLogger(CrawlerManager.class);
    private final MoamaProducer mainProducer;
    private final MoamaConsumer helperConsumer;

    private static int crawlerThreadPriority;
    private static int shuffleSize;
    private LinkedList<Thread> crawlerThreadList;
    private static int numOfThreads;
    private static int startNewThreadDelay;


    public CrawlerManager() {
        mainProducer = new MoamaProducer(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_LINK_TOPIC_NAME)
                , "kafka.server.");
        helperConsumer = new MoamaConsumer(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_HELPER_TOPIC_NAME)
                , "kafka.helper.");
        crawlerThreadPriority = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_THREAD_PRIORITY));
        shuffleSize = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_SHUFFLE_SIZE));
        numOfThreads = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_NUMBER_OF_THREADS));
        startNewThreadDelay = Integer.parseInt(ConfigManager.getInstance().getProperty(CrawlerPropertyType.CRAWLER_START_NEW_THREAD_DELAY_MS));
    }

    public void run() {
        crawlerThreadList = new LinkedList<>();
        for (int i = 0; i < numOfThreads; i++) {
            CrawlThread thread = new CrawlThread(true);
            thread.setPriority(crawlerThreadPriority);
            thread.start();
            crawlerThreadList.add(thread);
            try {
                //To make sure CPU can handle sudden start of many threads
                sleep(startNewThreadDelay);
            } catch (InterruptedException ignored) {
            }
        }
    }


    private void manageKafkaHelper() throws InterruptedException {
        LinkedList<String> linkedList = new LinkedList<>();
        while (true) {
            sleep(500);
            System.out.println("helperSize" + linkedList.size());
            linkedList.addAll(helperConsumer.getDocuments());
            if (linkedList.size() > shuffleSize) {
                Collections.shuffle(linkedList);
                mainProducer.pushNewURL(linkedList.toArray(new String[0]));
                linkedList.clear();
                System.out.println("shaffled");
            }
        }
    }

    @Override
    public void reconfigure() {
        // TODO: 9/1/18 mohammadreza
    }
}

