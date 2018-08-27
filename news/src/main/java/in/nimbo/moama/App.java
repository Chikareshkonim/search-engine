package in.nimbo.moama;

import in.nimbo.moama.fetcher.NewsFetcher;
import in.nimbo.moama.fetcher.NewsInfo;
import in.nimbo.moama.fetcher.RssReader;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws IOException {
        Queue<NewsInfo> news = new Queue<>(10000);
        RssReader reader = new RssReader(news);
        new Thread(reader).start();
        NewsFetcher fetcher = new NewsFetcher(news);
        new Thread(fetcher).start();
    }
}
