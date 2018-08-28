package in.nimbo.moama;

import in.nimbo.moama.fetcher.NewsFetcher;
import in.nimbo.moama.fetcher.NewsInfo;
import in.nimbo.moama.fetcher.RSSReader;
import newsutil.NewsConfigManager;

import static newsutil.NewsPropertyType.NEWS_QUEUE_CAPACITY;

/**
 * Hello world!
 *
 */
public class App {
    private static final int NEWS_CAPACITY = Integer.parseInt(NewsConfigManager.getInstance().getProperty(NEWS_QUEUE_CAPACITY));

    public static void main(String[] args) {
        Queue<NewsInfo> news = new Queue<>(NEWS_CAPACITY);
        RSSReader reader = new RSSReader(news);
        new Thread(reader).start();
        NewsFetcher fetcher = new NewsFetcher(news);
        new Thread(fetcher).start();
    }
}
