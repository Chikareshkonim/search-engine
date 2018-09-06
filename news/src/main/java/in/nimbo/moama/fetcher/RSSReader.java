package in.nimbo.moama.fetcher;

import in.nimbo.moama.RSSs;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.FloatMeter;

import java.io.IOException;

import static in.nimbo.moama.newsutil.NewsPropertyType.NEWS_READER_WAIT;

public class RSSReader implements Runnable {

    private NewsURLQueue<NewsInfo> newsQueue;
    private FloatMeter floatMeter = new FloatMeter("RSSReaderTime");
    private static final int SLEEP_TIME = Integer.parseInt(ConfigManager.getInstance().getProperty(NEWS_READER_WAIT));

    public RSSReader(NewsURLQueue<NewsInfo> newsQueue) {
        this.newsQueue = newsQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long initTime = System.currentTimeMillis();
            RSSs.getInstance().getRssToDomainMap().entrySet().stream().parallel().forEach(entry -> {
                if (RSSs.getInstance().isPolite(entry.getValue())) {
                    try {
                        newsQueue.addUrls(RSSParser.parse(entry.getKey(), entry.getValue()));
                        System.out.println("rss added to queue");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            floatMeter.add((float) (System.currentTimeMillis() - initTime) / 1000);
        }
    }
}
