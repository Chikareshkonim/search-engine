package in.nimbo.moama.fetcher;

import in.nimbo.moama.RSSs;
import in.nimbo.moama.metrics.FloatMeter;

import java.io.IOException;

public class RSSReader implements Runnable {

    private NewsURLQueue<NewsInfo> newsQueue;
    private FloatMeter floatMeter = new FloatMeter("RSSReaderTime");

    public RSSReader(NewsURLQueue<NewsInfo> newsQueue) {
        this.newsQueue = newsQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(200);
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
                floatMeter.add((System.currentTimeMillis() - initTime) / 1000);
            });
        }
    }
}
