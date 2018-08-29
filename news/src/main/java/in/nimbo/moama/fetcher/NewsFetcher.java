package in.nimbo.moama.fetcher;

import in.nimbo.moama.ElasticManager;
import in.nimbo.moama.NewsHBaseManager;
import in.nimbo.moama.configmanager.ConfigManager;

import java.io.IOException;
import java.util.LinkedList;

import static in.nimbo.moama.newsutil.NewsPropertyType.*;


public class NewsFetcher implements Runnable {
    private NewsURLQueue<NewsInfo> newsQueue;
    private NewsHBaseManager newsHBaseManager;
    private ElasticManager elasticManager;
    private static final int FETCHER_THREADS = Integer.parseInt(ConfigManager.getInstance().getProperty(NUMBER_OF_FETCHER_THREADS));
    private static final int FETCHER_PRIORITY = Integer.parseInt(ConfigManager.getInstance().getProperty(FETCHER_THREAD_PRIORITY));

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
                    System.out.println("fetching news");
                    if (list.size() < 1) {
                        try {
                            list.addAll(newsQueue.getUrls());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    try {
                        NewsInfo newsInfo = list.removeFirst();
                        String text = NewsParser.parse(newsInfo.getDomain(), newsInfo.getUrl());
                        News news = new News(newsInfo, text);
                        elasticManager.put(news.documentToJson(),null);
                        newsHBaseManager.put(news.documentToJson());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    // FIXME: 8/15/18
                }
            });
            thread.setPriority(FETCHER_PRIORITY);
            thread.start();
        }
    }
}
