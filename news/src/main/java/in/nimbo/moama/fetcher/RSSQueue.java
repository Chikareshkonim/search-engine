package in.nimbo.moama.fetcher;

import java.util.List;

public interface RSSQueue<S> {
    List<S> getUrls();

    void addUrls(List<S> url);

    int size();
}
