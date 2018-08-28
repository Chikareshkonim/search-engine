package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.fetcher.NewsFetcher;
import in.nimbo.moama.fetcher.NewsInfo;
import in.nimbo.moama.fetcher.NewsURLQueue;
import in.nimbo.moama.fetcher.RSSReader;

import java.io.IOException;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;
import static in.nimbo.moama.newsutil.NewsPropertyType.NEWS_QUEUE_CAPACITY;

/**
 * Hello world!
 *
 */
public class App {

    public static void main(String[] args) throws IOException {
        ConfigManager.getInstance().load(App.class.getResourceAsStream("/news.properties"), PROPERTIES);
        int newsCapacity = Integer.parseInt(ConfigManager.getInstance().getProperty(NEWS_QUEUE_CAPACITY));
        NewsURLQueue<NewsInfo> news = new Queue<>(newsCapacity);
        RSSReader reader = new RSSReader(news);
        System.out.println("created rss reader");
        new Thread(reader).start();
        NewsFetcher fetcher = new NewsFetcher(news);
        System.out.println("created news fetcher");
        new Thread(fetcher).start();
    }
}
