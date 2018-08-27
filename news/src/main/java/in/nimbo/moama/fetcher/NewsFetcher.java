package in.nimbo.moama.fetcher;

import java.io.IOException;
import java.util.LinkedList;

import static in.nimbo.moama.Config.FETCHER_THREAD_PRIORITY;
import static in.nimbo.moama.Config.NUMBER_OF_FETCHER_THREADS;


public class NewsFetcher implements Runnable {
    private NewsURLQueue<NewsInfo> newsQueue;

    public NewsFetcher(NewsURLQueue<NewsInfo> newsQueue) {
        this.newsQueue = newsQueue;
    }

    @Override
    public void run() {
        for (int i = 0; i < NUMBER_OF_FETCHER_THREADS; i++) {
            Thread thread = new Thread(() -> {
                LinkedList<NewsInfo> list = new LinkedList<>();
                while (true) {
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
                        System.out.println(news);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    // FIXME: 8/15/18
                }
            });
            thread.setPriority(FETCHER_THREAD_PRIORITY);
            thread.start();
        }
    }
}
