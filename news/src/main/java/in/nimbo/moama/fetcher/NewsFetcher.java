package in.nimbo.moama.fetcher;

import in.nimbo.moama.ElasticManager;
import in.nimbo.moama.NewsHBaseManager;
import in.nimbo.moama.RSSs;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.FloatMeter;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

import static in.nimbo.moama.newsutil.NewsPropertyType.*;


public class NewsFetcher implements Runnable {
    private NewsURLQueue<NewsInfo> newsQueue;
    private NewsHBaseManager newsHBaseManager;
    private ElasticManager elasticManager;
    private static final int FETCHER_THREADS = ConfigManager.getInstance().getIntProperty(NUMBER_OF_FETCHER_THREADS);
    private static final int FETCHER_PRIORITY = ConfigManager.getInstance().getIntProperty(FETCHER_THREAD_PRIORITY);
    private static final int SLEEP_TIME = ConfigManager.getInstance().getIntProperty(NEWS_FETCHER_WAIT);
    private static FloatMeter floatMeter = new FloatMeter("NewsFetcherTime");

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
                        e.printStackTrace();
                    }
                    long initTime = System.currentTimeMillis();
                    if (list.size() < 1) {
                        try {
                            list.addAll(newsQueue.getUrls());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    try {
                        NewsInfo newsInfo = list.removeFirst();
                        if (RSSs.getInstance().isPolite(newsInfo.getDomain())) {
                            String text = NewsParser.parse(newsInfo.getDomain(), newsInfo.getUrl());
                            News news = new News(newsInfo, text);
                            if (!RSSs.getInstance().isSeen(news.getNewsInfo().getUrl())) {
                                elasticManager.put(Collections.singletonList(news.getDocument()));
                                newsHBaseManager.put(news.documentToJson());
                            }
                            System.out.println("completed " + news.getNewsInfo().getUrl());
                        } else {
                            list.addLast(newsInfo);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    floatMeter.add((float) (System.currentTimeMillis() - initTime) / 1000);
                }
            });
            thread.setPriority(FETCHER_PRIORITY);
            thread.start();
        }
    }
}
