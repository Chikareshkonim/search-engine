package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.CrawlerManager;
import in.nimbo.moama.listener.Listener;
import in.nimbo.moama.util.CrawlerPropertyType;

import java.io.IOException;
import java.net.URISyntaxException;

public class App {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Initializer.initialize();
        System.out.println("1s");
        new Listener().listen(ListenerFunction.class,ConfigManager.getInstance().getIntProperty(CrawlerPropertyType.LISTENER_PORT));
        System.out.println("2s");
        CrawlerManager.getInstance().run();
    }
}