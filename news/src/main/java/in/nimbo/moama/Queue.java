package in.nimbo.moama;

import in.nimbo.moama.fetcher.RSSQueue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Queue<T> implements RSSQueue<T> {
    private ArrayBlockingQueue<T> queue;

    @Override
    public List<T> getUrls() {
        return Collections.singletonList(queue.poll());
    }

    @Override
    public void addUrls(List<T> urls) {
        urls.forEach(url -> {
            try {
                queue.put(url);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
    @Override
    public int size() {
        return queue.size();
    }
}
