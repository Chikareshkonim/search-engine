package in.nimbo.moama.fetcher;

import java.util.List;

public interface NewsURLQueue<S> {
    List<S> getUrls() throws InterruptedException;

    void addUrls(List<S> urls);

    int size();
}

