package in.nimbo.moama;


import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.CrawlerManager;
import in.nimbo.moama.listener.Listener;

import java.io.IOException;
import java.io.InputStream;

public class App {
    public static void main(String[] args) throws IOException {
        InputStream fileInputStream = CrawlerManager.class.getResourceAsStream("/crawler.properties");
        ConfigManager configManager=ConfigManager.getInstance();
        configManager.load(fileInputStream,ConfigManager.FileType.PROPERTIES);
        Initializer.initialize();
        new Listener().listen(ListenerFunction.class);
        CrawlerManager.getInstance().run();
    }
}