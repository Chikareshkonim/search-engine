package in.nimbo.moama.crawler;

import in.nimbo.moama.Utils;
import in.nimbo.moama.configmanager.*;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.kafka.MoamaProducer;
import in.nimbo.moama.util.CrawlerPropertyType;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class CrawlerManager implements Reconfigurable  {
    private static final Logger LOGGER = Logger.getLogger(CrawlerManager.class);
    private final MoamaProducer mainProducer;
    private final MoamaConsumer helperConsumer;
    private static int crawlerThreadPriority;
    private static int shuffleSize;
    private LinkedList<CrawlThread> crawlerThreadList = new LinkedList<>();
    private static int numOfThreads;
    private static int startNewThreadDelay;
    private static CrawlerManager ourInstance=new CrawlerManager();


    private boolean isRun=true;

    public static CrawlerManager getInstance() {
        return ourInstance;
    }

    private CrawlerManager() {
        mainProducer = new MoamaProducer(ConfigManager.getInstance()
                .getProperty(CrawlerPropertyType.CRAWLER_LINK_TOPIC_NAME), "kafka.server.");
        helperConsumer = new MoamaConsumer(ConfigManager.getInstance()
                .getProperty(CrawlerPropertyType.CRAWLER_HELPER_TOPIC_NAME), "kafka.helper.");
        crawlerThreadPriority = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_THREAD_PRIORITY);
        shuffleSize = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_SHUFFLE_SIZE);
        numOfThreads = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_NUMBER_OF_THREADS);
        startNewThreadDelay = ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.CRAWLER_START_NEW_THREAD_DELAY_MS);
    }
    public void run(){
        run(numOfThreads);
    }

    public void run(int numOfThreads) {
        IntStream.range(0,numOfThreads).mapToObj(e->new CrawlThread())
                .peek(thread->thread.setPriority(crawlerThreadPriority))
                .peek(e->Utils.delay(startNewThreadDelay))
                .peek(Thread::start)
                .forEach(crawlerThreadList::add);
        manageKafkaHelper();
    }


    private void manageKafkaHelper() {
        List<String> list = new LinkedList<>();
        while (isRun) {
            Utils.delay(500);
            System.out.println("helperSize" + list.size());
            list.addAll(helperConsumer.getDocuments());
            if (list.size() > shuffleSize) {
                Collections.shuffle(list);
                mainProducer.pushNewURL(list.toArray(new String[0]));
                list.clear();
                System.out.println("shuffled");
            }
        }
    }

    @Override
    public void reconfigure() {
        // TODO: 9/1/18 mohammadreza
    }

    public LinkedList<CrawlThread> getCrawlerThreadList() {
        return crawlerThreadList;
    }

    public void setRun(boolean run) {
        isRun = run;
    }
}

