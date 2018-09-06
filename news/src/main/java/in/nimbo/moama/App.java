package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.fetcher.NewsFetcher;
import in.nimbo.moama.fetcher.NewsInfo;
import in.nimbo.moama.fetcher.NewsURLQueue;
import in.nimbo.moama.fetcher.RSSReader;
import in.nimbo.moama.listener.Function;
import in.nimbo.moama.listener.Listener;
import org.apache.log4j.Logger;

import java.io.IOException;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;
import static in.nimbo.moama.newsutil.NewsPropertyType.*;

/**
 * Hello world!
 *
 */
public class App {
    private static final Logger LOGGER = Logger.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        ConfigManager.getInstance().load(App.class.getResourceAsStream("/news.properties"), PROPERTIES);
        new Listener().listen(Function.class, Integer.parseInt(ConfigManager.getInstance().getProperty(NEWS_LISTENER_PORT)));
        if (createTable()) {
            int newsCapacity = Integer.parseInt(ConfigManager.getInstance().getProperty(NEWS_QUEUE_CAPACITY));
            NewsURLQueue<NewsInfo> news = new Queue<>(newsCapacity);
            RSSReader reader = new RSSReader(news);
            LOGGER.trace("websites table is up");
            new Thread(reader).start();
            NewsFetcher fetcher = new NewsFetcher(news);
            LOGGER.trace("created news fetchers");
            new Thread(fetcher).start();
        } else {
            System.out.println("Failed to create table!");
        }
    }

    private static boolean createTable() {
        ConfigManager configManager = ConfigManager.getInstance();
        NewsWebsiteHBaseManager webHBaseManager = new NewsWebsiteHBaseManager(configManager.getProperty(NEWS_WEBSITE_TABLE),
                configManager.getProperty(HBASE_TEMPLATE_FAMILY), configManager.getProperty(HBASE_RSS_FAMILY));
        return webHBaseManager.createTable(null);
    }
}
