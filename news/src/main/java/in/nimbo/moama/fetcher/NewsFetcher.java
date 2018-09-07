package in.nimbo.moama.fetcher;

import in.nimbo.moama.ElasticManager;
import in.nimbo.moama.NewsHBaseManager;
import in.nimbo.moama.RSSs;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.FloatMeter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static in.nimbo.moama.newsutil.NewsPropertyType.*;


public class NewsFetcher implements Runnable {
    private NewsURLQueue<NewsInfo> newsQueue;
    private NewsHBaseManager newsHBaseManager;
    private ElasticManager elasticManager;
    private static final int FETCHER_THREADS = Integer.parseInt(ConfigManager.getInstance().getProperty(NUMBER_OF_FETCHER_THREADS));
    private static final int FETCHER_PRIORITY = Integer.parseInt(ConfigManager.getInstance().getProperty(FETCHER_THREAD_PRIORITY));
    private static final int SLEEP_TIME = Integer.parseInt(ConfigManager.getInstance().getProperty(NEWS_FETCHER_WAIT));
    private static FloatMeter floatMeter = new FloatMeter("NewsFetcherTime");
    private static final Logger LOGGER = Logger.getLogger(NewsFetcher.class);


    public NewsFetcher(NewsURLQueue<NewsInfo> newsQueue) {
        this.newsQueue = newsQueue;
        ConfigManager configManager = ConfigManager.getInstance();
        newsHBaseManager = new NewsHBaseManager(configManager.getProperty(NEWS_PAGES_TABLE),
                configManager.getProperty(HBASE_TWITTER_FAMILY), configManager.getProperty(HBASE_VISITED_FAMILY));
        elasticManager = new ElasticManager();
    }

    @Override
    public void run() {
        for (int i = 0; i < FETCHER_THREADS; i++) {
            Thread thread = new Thread(() -> {
                LinkedList<NewsInfo> list = new LinkedList<>();
                while (true) {
                    try {
                        Thread.sleep(SLEEP_TIME);
                    } catch (InterruptedException e) {
                        LOGGER.error("Interrupt exception at NewsFetcher", e);
                    }
                    long initTime = System.currentTimeMillis();
                    if (list.size() < 1) {
                        try {
                            list.addAll(newsQueue.getUrls());
                        } catch (InterruptedException e) {
                            LOGGER.error("Interrupt exception at NewsFetcher", e);
                        }
                    }
                    try {
                        NewsInfo newsInfo = list.removeFirst();
                        if (RSSs.getInstance().isPolite(newsInfo.getDomain())) {
                            String text = NewsParser.parse(newsInfo.getDomain(), newsInfo.getUrl());
                            News news = new News(newsInfo, text);
                            if (!RSSs.getInstance().isSeen(news.getNewsInfo().getUrl())) {
                                addToDBs(Collections.singletonList(news));
                            }
                            LOGGER.trace("Completed: " + news.getNewsInfo().getUrl());
                        } else {
                            list.addLast(newsInfo);
                        }
                    } catch (IOException e) {
                        LOGGER.error("IOException at NewsFetcher", e);
                    }
                    floatMeter.add((float) (System.currentTimeMillis() - initTime) / 1000);
                }
            });
            thread.setPriority(FETCHER_PRIORITY);
            thread.start();
        }
    }

    private void addToDBs(List<News> newsList) {
        List<Put> puts = new ArrayList<>();
        List<Map<String, String>> docs = new ArrayList<>();
        newsList.forEach(news -> {
            Put put = new Put(newsHBaseManager.generateRowKeyFromUrl(news.getNewsInfo().getUrl()).getBytes());
            ConfigManager configManager = ConfigManager.getInstance();
            put.addColumn(configManager.getProperty(HBASE_VISITED_FAMILY).getBytes(),
                    configManager.getProperty(HBASE_VISITED_FAMILY).getBytes(), new byte[0]);
            docs.add(news.getDocument());
            puts.add(put);
        });
        elasticManager.myput(docs);
        newsHBaseManager.put(puts);
    }
}
