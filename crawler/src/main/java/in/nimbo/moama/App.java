package in.nimbo.moama;


import in.nimbo.moama.crawler.CrawlerManager;
import in.nimbo.moama.listener.Listener;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        Initializer.initialize();
        new Listener().listen(ListenerFunction.class,2719);
        CrawlerManager.getInstance().run();
    }
}