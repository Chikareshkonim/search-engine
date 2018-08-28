package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.fetcher.NewsFetcher;
import in.nimbo.moama.fetcher.NewsInfo;
import in.nimbo.moama.fetcher.NewsURLQueue;
import in.nimbo.moama.fetcher.RSSReader;

import java.io.IOException;

import static newsutil.NewsPropertyType.NEWS_QUEUE_CAPACITY;

/**
 * Hello world!
 *
 */
public class App {

    public static void main(String[] args) throws IOException {
        ConfigManager.getInstance().load(App.class.getResourceAsStream("/news.properties"), ConfigManager.FileType.PROPERTIES);
        int newsCapacity = Integer.parseInt(ConfigManager.getInstance().getProperty(NEWS_QUEUE_CAPACITY));
        NewsURLQueue<NewsInfo> news = new Queue<>(newsCapacity);
        RSSReader reader = new RSSReader(news);
        new Thread(reader).start();
        NewsFetcher fetcher = new NewsFetcher(news);
        new Thread(fetcher).start();
    }
}
