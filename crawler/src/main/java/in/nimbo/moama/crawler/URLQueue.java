package in.nimbo.moama.crawler;

import java.util.List;

public interface URLQueue {
    List<String> getUrls();

    void pushNewURL(String... links);
}
